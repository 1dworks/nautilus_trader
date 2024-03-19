import pandas as pd
from dataclasses import dataclass

from nautilus_trader.common.actor import Actor
from nautilus_trader.common.component import TimeEvent
from nautilus_trader.continuous.config import ContractChainConfig
from nautilus_trader.continuous.contract_month import ContractMonth
from nautilus_trader.core.datetime import unix_nanos_to_dt
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.continuous.bar import ContinuousBar

@dataclass
class RollEvent:
    from_instrument_id: InstrumentId
    to_instrument_id: InstrumentId

class ContractExpired(Exception):
    pass


class ContractChain(Actor):
    def __init__(
        self,
        config: ContractChainConfig,
    ):

        super().__init__()

        self.bar_type = config.bar_type
        self.rolls = pd.DataFrame(columns=["timestamp", "to_month"])
        self.current_month: ContractMonth | None = None
        self.previous_month: ContractMonth | None = None
        self.forward_month: ContractMonth | None = None
        self.carry_month: ContractMonth | None = None
        self.current_bar_type: BarType | None = None
        self.previous_bar_type: BarType | None = None
        self.forward_bar_type: BarType | None = None
        self.carry_bar_type: BarType | None = None
        self.expiry_date: pd.Timestamp | None = None
        self.roll_date: pd.Timestamp | None = None

        self._roll_offset = config.roll_config.roll_offset
        self._carry_offset = config.roll_config.carry_offset
        self._priced_cycle = config.roll_config.priced_cycle
        self._hold_cycle = config.roll_config.hold_cycle
        self._approximate_expiry_offset = config.roll_config.approximate_expiry_offset

        assert self._roll_offset <= 0
        assert self._carry_offset == 1 or self._carry_offset == -1

        self._start_month = config.start_month
        self._raise_expired = config.raise_expired
        self._ignore_expiry_date = config.ignore_expiry_date
        self._instrument_id = self.bar_type.instrument_id

        assert self._start_month in self._hold_cycle
        
        self._last_current: Bar | None = None
        self._last_forward: Bar | None = None
        self._last_carry: Bar | None = None
        self._last_previous: Bar | None = None
        
    def current_bar(self) -> Bar:
        return self.cache.bar(self.current_bar_type, 0)
    
    def forward_bar(self) -> Bar:
        return self.cache.bar(self.forward_bar_type, 0)
    
    def carry_bar(self) -> Bar:
        return self.cache.bar(self.carry_bar_type, 0)
    
    def previous_bar(self) -> Bar:
        return self.cache.bar(self.previous_bar_type, 0)
        
    def on_start(self) -> None:

        self._roll(to_month=self._start_month)

        interval = self.bar_type.spec.timedelta
        now = unix_nanos_to_dt(self.clock.timestamp_ns())
        start_time = now.floor(interval) - interval + pd.Timedelta(seconds=5)

        self.clock.set_timer(
            name=f"chain_{self.bar_type}",
            interval=interval,
            start_time=start_time,
            callback=self.handle_time_event,
        )

    def handle_time_event(self, event: TimeEvent) -> None:
        self._attempt_roll()
        if self._raise_expired:
            self._raise_expiry()
        self._publish()

    def _publish(self) -> None:
        self._publish_forward_bar()
        self._publish_carry_bar()
        self._publish_previous_bar()
        self._publish_current_bar()
        self._publish_continuous_bar()
        
    def _publish_continuous_bar(self) -> None:
        current_bar = self.cache.bar(self.current_bar_type)
        if current_bar is None:
            return
        
        bar = ContinuousBar(
            bar_type=self.bar_type,
            current_bar=current_bar,
            forward_bar=self.cache.bar(self.forward_bar_type),
            previous_bar=self.cache.bar(self.previous_bar_type),
            carry_bar=self.cache.bar(self.carry_bar_type),
            ts_init=self.clock.timestamp_ns(),
            ts_event=self.clock.timestamp_ns(),
        )
        
        self.msgbus.publish(
            topic=str(self.bar_type),
            msg=bar,
        )
    
    def _publish_current_bar(self) -> None:
        current_bar = self.cache.bar(self.current_bar_type)
        if current_bar is None:
            return

        is_last = self._last_current is not None and current_bar == self._last_current
        if is_last:
            return

        self.msgbus.publish(
            topic=f"{self.bar_type}",
            msg=current_bar,
        )

    def _publish_forward_bar(self) -> None:

        forward_bar = self.cache.bar(self.forward_bar_type)
        if forward_bar is None:
            return

        is_last = self._last_forward is not None and forward_bar == self._last_forward
        if is_last:
            return

        self.msgbus.publish(
            topic=f"{self.bar_type}+1",
            msg=forward_bar,
        )

    def _publish_carry_bar(self) -> None:

        carry_bar = self.cache.bar(self.carry_bar_type)
        if carry_bar is None:
            return

        is_last = self._last_carry is not None and carry_bar == self._last_carry
        if is_last:
            return

        self.msgbus.publish(
            topic=f"{self.bar_type}c",
            msg=carry_bar,
        )
        
    def _publish_previous_bar(self) -> None:

        previous_bar = self.cache.bar(self.previous_bar_type)
        if previous_bar is None:
            return

        is_last = self._last_previous is not None and previous_bar == self._last_previous
        if is_last:
            return

        self.msgbus.publish(
            topic=f"{self.bar_type}-1",
            msg=previous_bar,
        )
        
    def _raise_expiry(self):

        current_bar = self.cache.bar(self.current_bar_type)
        forward_bar = self.cache.bar(self.forward_bar_type)

        if current_bar is None and forward_bar is None:
            return

        timestamps = []
        if current_bar is not None:
            timestamps.append(unix_nanos_to_dt(current_bar.ts_event))

        if forward_bar is not None:
            timestamps.append(unix_nanos_to_dt(forward_bar.ts_event))

        timestamp = max(timestamps)

        is_expired = timestamp >= self.expiry_date
        if is_expired:
            raise ContractExpired(
                f"The chain failed to roll from {self.current_month} to {self.forward_month} before expiry date {self.expiry_date}",
            )

    def _attempt_roll(self) -> None:

        current_bar = self.cache.bar(self.current_bar_type)
        forward_bar = self.cache.bar(self.forward_bar_type)

        if current_bar is None or forward_bar is None:
            return

        forward_timestamp = unix_nanos_to_dt(forward_bar.ts_event)
        current_timestamp = unix_nanos_to_dt(current_bar.ts_event)

        if current_timestamp != forward_timestamp:
            return

        if self._ignore_expiry_date:
            in_roll_window = current_timestamp >= self.roll_date
        else:
            in_roll_window = (
                current_timestamp >= self.roll_date
            ) and current_timestamp < self.expiry_date

        if not in_roll_window:
            return

        self.roll()
        self.rolls.loc[len(self.rolls)] = (current_timestamp, self.current_month)

    def roll(self) -> None:
        """
        Roll to the next month in the chain.
        """
        to_month = self._hold_cycle.next_month(self.current_month)
        self._roll(to_month=to_month)
        self.msgbus.publish(
            topic=f"events.roll.{self.bar_type}",
            msg=RollEvent(
                from_instrument_id=self.previous_bar_type,
                to_instrument_id=self.current_bar_type,
            ),
        )

    def _roll(self, to_month: ContractMonth) -> None:
        self._update_attributes(to_month=to_month)
        self._update_subscriptions()
        self._log.debug(
            f"Rolled {self.previous_bar_type.instrument_id} > {self.current_bar_type.instrument_id}",
        )

    def _update_subscriptions(self) -> None:
        """
        Update the subscriptions after the roll.
        """
        self._log.info("Updating subscriptions...")
        self.unsubscribe_bars(self.previous_bar_type)
        self.subscribe_bars(self.current_bar_type)
        self.subscribe_bars(self.forward_bar_type)

    def _update_attributes(self, to_month: ContractMonth) -> None:

        self.current_month = to_month
        self.previous_month = self._hold_cycle.previous_month(self.current_month)
        self.forward_month = self._hold_cycle.next_month(self.current_month)
        if self._carry_offset == 1:
            self.carry_month = self._priced_cycle.next_month(self.current_month)
        elif self._carry_offset == -1:
            self.carry_month = self._priced_cycle.previous_month(self.current_month)

        self.current_bar_type = self._make_bar_type(self.current_month)
        self.previous_bar_type = self._make_bar_type(self.previous_month)
        self.forward_bar_type = self._make_bar_type(self.forward_month)
        self.carry_bar_type = self._make_bar_type(self.carry_month)

        self.roll_date, self.expiry_date = self.current_month.roll_window(
            self._approximate_expiry_offset,
            self._roll_offset,
        )

    def _make_bar_type(self, month: ContractMonth) -> BarType:
        return BarType(
            instrument_id=self._fmt_instrument_id(month),
            bar_spec=self.bar_type.spec,
            aggregation_source=self.bar_type.aggregation_source,
        )

    def _fmt_instrument_id(self, month: ContractMonth) -> InstrumentId:
        """
        Format the InstrumentId for contract given the ContractMonth.
        """
        symbol = self._instrument_id.symbol.value
        venue = self._instrument_id.venue.value
        return InstrumentId.from_str(
            f"{symbol}={month.year}{month.letter_month}.{venue}",
        )

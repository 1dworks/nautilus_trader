import itertools

from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import MessageBus
from nautilus_trader.common.component import TestClock
from nautilus_trader.common.component import TimeEvent
from nautilus_trader.continuous.chain import ContractChain
from nautilus_trader.continuous.config import ContractChainConfig
from nautilus_trader.continuous.contract_month import ContractMonth
from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.data.engine import DataEngine
from nautilus_trader.model.data import Bar
from nautilus_trader.portfolio.portfolio import Portfolio
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs
from nautilus_trader.continuous.bar import ContinuousBar
from nautilus_trader.core.correctness import PyCondition

class ContinuousBarWrangler:
    """
    end_month: ContractMonth
        The end month in the hold cycle the wrangler should stop at. The data
        returned from the wrangler will include the end month (inclusive).
    """

    def __init__(
        self,
        config: ContractChainConfig,
        end_month: ContractMonth,
    ):
        
        PyCondition.type(config, ContractChainConfig, "config")
        PyCondition.type(end_month, ContractMonth, "end_month")

        self._clock = TestClock()

        self._msgbus = MessageBus(
            trader_id=TestIdStubs.trader_id(),
            clock=self._clock,
        )

        self._cache = Cache()

        portfolio = Portfolio(
            self._msgbus,
            self._cache,
            self._clock,
        )
        self._chain_config = config
        self._chain = ContractChain(config=self._chain_config)

        self._chain.register_base(
            portfolio=portfolio,
            msgbus=self._msgbus,
            cache=self._cache,
            clock=self._clock,
        )

        self._data_engine = DataEngine(
            msgbus=self._msgbus,
            cache=self._cache,
            clock=self._clock,
        )

        self._data_engine.start()

        self._end_month = end_month

        self._start_month = self._chain_config.start_month
        assert self._start_month is not None
        assert self._end_month > self._start_month

        self._roll_config = self._chain_config.roll_config
        self._expiry_offset = self._roll_config.approximate_expiry_offset
        self._roll_offset = self._roll_config.roll_offset

    def process(
        self,
        bars: list[Bar],
    ) -> list[ContinuousBar]:

        bars = sorted(bars, key=lambda x: x.ts_init)

        self.validate(bars)

        self._clock.set_time(bars[0].ts_init)

        self._chain.start()

        results: dict[str, list[Bar]] = {}

        results = []
        self._msgbus.subscribe(
            topic=f"{self._chain.bar_type}",
            handler=results.append,
        )

        previous_timestamp = None

        for bar in bars:

            is_next = previous_timestamp is not None and bar.ts_init > previous_timestamp
            if is_next:
                self._chain.handle_time_event(
                    TimeEvent(
                        name=f"chain_{self._chain.bar_type}",
                        event_id=UUID4(),
                        ts_event=0,
                        ts_init=0,
                    ),
                )
                if self._chain.current_month == self._end_month:
                    break
            
            self._cache.add_bar(bar)
            
            self._data_engine.process(bar)

            previous_timestamp = bar.ts_init

        return results

    def validate(self, bars: list[Bar]) -> None:
        """
        Validate the contract bars for a successful roll from the start to end month.
        """
        venues = {b.bar_type.instrument_id.venue for b in bars}
        assert len(venues) == 1

        specs = {b.bar_type.spec for b in bars}
        assert len(specs) == 1

        try:
            for bar in bars:
                symbol = bar.bar_type.instrument_id.symbol.value
                ContractMonth(symbol.split("=")[-1])
        except AssertionError:
            raise ValueError(
                f"Symbol {symbol} has incorrect format. The format should is <symbol>=<month>",
            )

        timestamps_by_month = {}
        for bar in bars:
            month = bar.bar_type.instrument_id.symbol.value.split("=")[-1]
            if timestamps_by_month.get(month) is None:
                timestamps_by_month[month] = set()
            timestamps_by_month[month].add(bar.ts_init)

        hold_cycle = self._chain_config.roll_config.hold_cycle

        months = list(hold_cycle.iterate(self._start_month, self._end_month))

        missing = [
            m.value for m in [*months, self._end_month] if timestamps_by_month.get(m.value) is None
        ]
        
        symbol = "=".join(
            self._chain_config.bar_type.instrument_id.symbol.value.split("=")[:-2],
        )
        if len(missing) > 0:
            raise ValueError(f"Data validation failed: {symbol} has no timestamps in months {missing}")
        
        for current_month in months:

            start, end = current_month.roll_window(
                approximate_expiry_offset=self._expiry_offset,
                roll_offset=self._roll_offset,
            )

            start_ns = dt_to_unix_nanos(start)
            end_ns = dt_to_unix_nanos(end)

            # check current contract timestamps exist in roll window
            current_timestamps = {
                t for t in timestamps_by_month[current_month.value] if t >= start_ns and t < end_ns
            }
            if len(current_timestamps) == 0:
                raise ValueError(
                    f"Data validation failed: {symbol}={current_month} has no timestamps in roll window {start} to {end}",
                )

            # check forward contract timestamps exist in roll window
            forward_month = hold_cycle.next_month(current_month)
            forward_timestamps = {
                t for t in timestamps_by_month[forward_month.value] if t >= start_ns and t < end_ns
            }
            if len(forward_timestamps) == 0:
                raise ValueError(
                    f"Data validation failed: {symbol}={forward_month} has no timestamps in roll window {start} to {end}",
                )

            # check matching timestamps for current and forward contract exist in roll window
            is_matching = len(current_timestamps & forward_timestamps) > 0
            if not is_matching:
                raise ValueError(
                    f"Data validation failed: {symbol}: {current_month} and {forward_month} have no matching timestamps in roll window {start} to {end}",
                )

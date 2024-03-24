import pandas as pd
import pytest

from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.backtest.engine import BacktestEngineConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.continuous.chain import ContractChain
from nautilus_trader.continuous.config import ContractChainConfig
from nautilus_trader.continuous.config import RollConfig
from nautilus_trader.continuous.contract_month import LETTER_MONTHS
from nautilus_trader.continuous.contract_month import ContractMonth
from nautilus_trader.continuous.cycle import RollCycle
from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.core.datetime import unix_nanos_to_dt
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import AssetClass
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.instruments.futures_contract import FuturesContract
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.continuous.chain import ContractExpired

class TestContractChain:
    def setup(self):

        config = BacktestEngineConfig(
            logging=LoggingConfig(bypass_logging=True),
            run_analysis=False,
        )

        self.engine = BacktestEngine(config=config)

        self.engine.add_venue(
            venue=Venue("SIM"),
            oms_type=OmsType.HEDGING,
            account_type=AccountType.MARGIN,
            base_currency=USD,
            starting_balances=[Money(1_000_000, USD)],
        )

        self.chain_config = ContractChainConfig(
            bar_type=BarType.from_str("MES.SIM-1-DAY-MID-EXTERNAL"),
            roll_config=RollConfig(
                hold_cycle=RollCycle("HMUZ"),
                priced_cycle=RollCycle("FGHJKMNQUVXZ"),
                roll_offset=-5,
                approximate_expiry_offset=14,
                carry_offset=1,
            ),
            start_month=ContractMonth("2021H"),
        )

        for letter_month in LETTER_MONTHS:
            self.engine.add_instrument(
                FuturesContract(
                    instrument_id=InstrumentId.from_str(f"MES=2021{letter_month}.SIM"),
                    raw_symbol=Symbol("MES"),
                    asset_class=AssetClass.COMMODITY,
                    currency=Currency.from_str("GBP"),
                    price_precision=4,
                    price_increment=Price.from_str("0.0001"),
                    multiplier=Quantity.from_int(1),
                    lot_size=Quantity.from_int(1),
                    underlying="MES",
                    activation_ns=0,
                    expiration_ns=0,
                    ts_event=0,
                    ts_init=0,
                ),
            )

        self.msgbus = self.engine.kernel.msgbus

    def test_initialize_sets_expected_attributes(self):

        # Arrange
        chain = ContractChain(config=self.chain_config)

        self.engine.add_actor(chain)

        data = [
            ("MES=2021H.SIM", "2021-03-09"),
            ("MES=2021M.SIM", "2021-03-10"),
        ]

        bars = self._create_bars(data)
        self.engine.add_data(bars, validate=False)

        # Act
        self.engine.run()

        # Assert
        assert len(chain.rolls) == 0

        assert chain.current_month == ContractMonth("2021H")
        assert chain.previous_month == ContractMonth("2020Z")
        assert chain.forward_month == ContractMonth("2021M")
        assert chain.carry_month == ContractMonth("2021J")

        assert chain.expiry_date == pd.Timestamp("2021-03-15", tz="UTC")
        assert chain.roll_date == pd.Timestamp("2021-03-10", tz="UTC")

        assert chain.current_bar_type == BarType.from_str("MES=2021H.SIM-1-DAY-MID-EXTERNAL")
        assert chain.previous_bar_type == BarType.from_str("MES=2020Z.SIM-1-DAY-MID-EXTERNAL")
        assert chain.forward_bar_type == BarType.from_str("MES=2021M.SIM-1-DAY-MID-EXTERNAL")
        assert chain.carry_bar_type == BarType.from_str("MES=2021J.SIM-1-DAY-MID-EXTERNAL")

        sub_topics = [
            s.topic for s in self.msgbus.subscriptions() if s.topic.startswith("data.bars")
        ]
        assert sub_topics == [
            "data.bars.MES=2021H.SIM-1-DAY-MID-EXTERNAL",
            "data.bars.MES=2021M.SIM-1-DAY-MID-EXTERNAL",
        ]

    def test_roll_sets_expected_attributes(self):

        # Arrange
        chain = ContractChain(config=self.chain_config)

        self.engine.add_actor(chain)

        data = [
            ("MES=2021H.SIM", "2021-03-09"),
            ("MES=2021M.SIM", "2021-03-09"),
            ("MES=2021H.SIM", "2021-03-10"),
            ("MES=2021M.SIM", "2021-03-10"),
            ("MES=2021M.SIM", "2021-03-11"),  # rolled
        ]

        bars = self._create_bars(data)
        self.engine.add_data(bars)

        # Act
        self.engine.run()

        # Assert
        assert len(chain.rolls) == 1
        assert chain.rolls.timestamp.iloc[0] == pd.Timestamp("2021-03-10", tz="UTC")
        assert chain.rolls.to_month.iloc[0] == ContractMonth("2021M")

        assert chain.current_month == ContractMonth("2021M")
        assert chain.previous_month == ContractMonth("2021H")
        assert chain.forward_month == ContractMonth("2021U")
        assert chain.carry_month == ContractMonth("2021N")

        assert chain.expiry_date == pd.Timestamp("2021-06-15", tz="UTC")
        assert chain.roll_date == pd.Timestamp("2021-06-10", tz="UTC")

        assert chain.current_bar_type == BarType.from_str("MES=2021M.SIM-1-DAY-MID-EXTERNAL")
        assert chain.previous_bar_type == BarType.from_str("MES=2021H.SIM-1-DAY-MID-EXTERNAL")
        assert chain.forward_bar_type == BarType.from_str("MES=2021U.SIM-1-DAY-MID-EXTERNAL")
        assert chain.carry_bar_type == BarType.from_str("MES=2021N.SIM-1-DAY-MID-EXTERNAL")

        sub_topics = [
            s.topic for s in self.msgbus.subscriptions() if s.topic.startswith("data.bars")
        ]
        assert sub_topics == [
            "data.bars.MES=2021M.SIM-1-DAY-MID-EXTERNAL",
            "data.bars.MES=2021U.SIM-1-DAY-MID-EXTERNAL",
        ]

    def test_current_bar_publish(self):

        # Arrange
        chain = ContractChain(config=self.chain_config)

        self.engine.add_actor(chain)

        results: list[Bar] = []
        self.msgbus.subscribe(
            topic=f"{chain.bar_type}",
            handler=results.append,
        )

        data = [
            ("MES=2021H.SIM", "2021-03-09"), # 0
            ("MES=2021M.SIM", "2021-03-09"), # 1
            ("MES=2021J.SIM", "2021-03-09"), # 2
            # ContinuousBar
            ("MES=2021H.SIM", "2021-03-10"), # 3
            ("MES=2021M.SIM", "2021-03-10"), # 4
            ("MES=2021U.SIM", "2021-03-10"), # 5
            ("MES=2021N.SIM", "2021-03-10"), # 6
            # ContinuousBar
            ("MES=2021M.SIM", "2021-03-11"), # 7
        ]

        bars = self._create_bars(data)
        self.engine.add_data(bars)

        # Act
        self.engine.run()

        # Assert
        assert len(results) == 2
        assert unix_nanos_to_dt(results[0].ts_init) == pd.Timestamp("2021-03-09 00:00:05+0000", tz="UTC")
        assert unix_nanos_to_dt(results[1].ts_init) == pd.Timestamp("2021-03-10 00:00:05+0000", tz="UTC")
        
        assert results[0].current_bar == bars[0]
        assert results[0].forward_bar == bars[1]
        assert results[0].previous_bar is None
        assert results[0].carry_bar == bars[2]
        
        assert results[1].current_bar == bars[4]
        assert results[1].forward_bar == bars[5]
        assert results[1].previous_bar == bars[3]
        assert results[1].carry_bar == bars[6]

    def test_contract_expired_raises(self):

        # Arrange
        chain = ContractChain(config=self.chain_config)

        self.engine.add_actor(chain)

        data = [
            ("MES=2021H.SIM", "2021-03-14"),
            ("MES=2021H.SIM", "2021-03-15"),  # expired
            ("MES=2021H.SIM", "2021-03-16"),
        ]

        bars = self._create_bars(data)
        self.engine.add_data(bars)

        # Act & Assert
        with pytest.raises(ContractExpired):
            self.engine.run()

    def _create_bars(self, data: list[tuple]) -> list[Bar]:
        return [
            Bar(
                bar_type=BarType.from_str(f"{row[0]}-1-DAY-MID-EXTERNAL"),
                open=Price.from_str("1.1"),
                high=Price.from_str("1.2"),
                low=Price.from_str("1.0"),
                close=Price.from_str("1.1"),
                volume=Quantity.from_int(1),
                ts_event=dt_to_unix_nanos(pd.Timestamp(row[1], tz="UTC")),
                ts_init=dt_to_unix_nanos(pd.Timestamp(row[1], tz="UTC")),
            )
            for row in data
        ]

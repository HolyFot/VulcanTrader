# flake8: noqa: F401
# isort: off
from VulcanTrader.resolvers.iresolver import IResolver
from VulcanTrader.resolvers.exchange_resolver import ExchangeResolver

# isort: on
# Don't import HyperoptResolver to avoid loading the whole Optimize tree
# from VulcanTrader.resolvers.hyperopt_resolver import HyperOptResolver
from VulcanTrader.resolvers.pairlist_resolver import PairListResolver
from VulcanTrader.resolvers.protection_resolver import ProtectionResolver
from VulcanTrader.resolvers.strategy_resolver import StrategyResolver

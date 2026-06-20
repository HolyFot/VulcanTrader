"""Check whether the configured exchange is supported."""

import logging

from VulcanTrader.constants import Config
from VulcanTrader.enums import RunMode
from VulcanTrader.exchange import available_exchanges, is_exchange_known_ccxt, validate_exchange
from VulcanTrader.exchange.common import MAP_EXCHANGE_CHILDCLASS, SUPPORTED_EXCHANGES
from VulcanTrader.util.exceptions import OperationalException


logger = logging.getLogger(__name__)


def check_exchange(config: Config, check_for_bad: bool = True) -> bool:
    """
    Check if the exchange name in the config file is supported by VulcanTrader.

    :param check_for_bad: if True, raise on known-bad exchanges. Otherwise warn.
    :return: True if the exchange is OK, False if it is a known-bad exchange
             (and ``check_for_bad`` is False). Raises ``OperationalException``
             when the exchange is unknown to ccxt or no exchange is configured.
    """

    if config["runmode"] in [
        RunMode.PLOT,
        RunMode.UTIL_NO_EXCHANGE,
        RunMode.OTHER,
    ] and not config.get("exchange", {}).get("name"):
        return True

    logger.info("Checking exchange...")

    exchange = config.get("exchange", {}).get("name", "").lower()
    if not exchange:
        raise OperationalException(
            "This command requires a configured exchange. You should either use "
            "`--exchange <exchange_name>` or specify a configuration file via `--config`.\n"
            f"The following exchanges are available: {', '.join(available_exchanges())}"
        )

    if not is_exchange_known_ccxt(exchange):
        raise OperationalException(
            f'Exchange "{exchange}" is not known to the ccxt library '
            "and therefore not available for the bot.\n"
            f"The following exchanges are available: {', '.join(available_exchanges())}"
        )

    valid, reason, _ = validate_exchange(exchange)
    if not valid:
        if check_for_bad:
            raise OperationalException(
                f'Exchange "{exchange}" will not work with VulcanTrader. Reason: {reason}.'
            )
        else:
            logger.warning(
                f'Exchange "{exchange}" will not work with VulcanTrader. Reason: {reason}.'
            )

    if MAP_EXCHANGE_CHILDCLASS.get(exchange, exchange) in SUPPORTED_EXCHANGES:
        logger.info(f'Exchange "{exchange}" is officially supported.')
    else:
        logger.warning(
            f'Exchange "{exchange}" is known to the ccxt library, '
            "available for the bot, but not officially supported. "
            "It may work flawlessly (please report back) or have serious issues. "
            "Use it at your own discretion."
        )

    return True

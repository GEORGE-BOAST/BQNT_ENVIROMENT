import bqbreg

__IS_ENABLED_FOR_BQMONITOR_DEVTOOLS = None
__IS_ENABLED_FOR_BQMONITOR = None


def is_enabled_for_bqmonitor_devtools() -> bool:
    """
    Description
    -----------
    Returns true if and only if the current user is enabled to use bqmonitor devtools
    such as the BCS Store.

    Returns
    -------
    bool :
        If the current user is enabled for bqmonitor developer tools
    """
    global __IS_ENABLED_FOR_BQMONITOR_DEVTOOLS
    if __IS_ENABLED_FOR_BQMONITOR_DEVTOOLS is None:
        __IS_ENABLED_FOR_BQMONITOR_DEVTOOLS = bqbreg.bqbreg_eval_boolean(
            "bbit_enable_bqmonitor_developer_tools", 453786, default=False
        )
    return __IS_ENABLED_FOR_BQMONITOR_DEVTOOLS


def is_enabled_for_bqmonitor() -> bool:
    """
    Description
    -----------
    Returns true if and only if the current user is enabled to use bqmonitor.

    Returns
    -------
    bool :
        If the current user is enabled for bqmonitor
    """
    global __IS_ENABLED_FOR_BQMONITOR
    if __IS_ENABLED_FOR_BQMONITOR is None:
        __IS_ENABLED_FOR_BQMONITOR = bqbreg.bqbreg_eval_boolean(
            "bbit_enable_bqmonitor", 452769, default=False
        )
    return __IS_ENABLED_FOR_BQMONITOR

__all__ = ("link",)


def link(source, target, unlink: bool = True):
    """Link a file or directory to another location.

    Args:
        source (str): Source file or directory.
        target (str): Target file or directory.
        unlink (bool, optional): Unlink the target if it exists. Defaults to True.

    Returns:
        str: Command to run.
    """
    if unlink:
        return f"ln -sfn {source} {target}"
    else:
        return f"ln -s {source} {target}"

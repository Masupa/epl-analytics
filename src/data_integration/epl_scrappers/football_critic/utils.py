""" Utility functions for the ELT """

from datetime import datetime
import numpy as np


def fill_missing_attribute_with_nan(info_dict: str) -> dict:
    """Compare the length of `Player Names` with that
    of other attributes, and fill in attributes with
    not matching length with NaN values.
    
    Args:
    -----
    info_dict : dict
        Dictionary with player information

    Returns:
        info_dict : dict
    """

    player_atts = [
        'Club', 'Nationality', 'Age', 'Position', 'Prefered Foot',
        'Height', 'Weight', 'Previous Teams'
        ]

    for att in player_atts:
        if len(info_dict['Player Names']) != len(info_dict[att]):
            info_dict[att].append(np.nan)

    return info_dict


def add_player_details(details: list, info_dict: dict) -> dict:
    """Takes a list of player details, extracts
    the relevant player details from it, add it's to
    the `info_dict` dictionary. In the case that
    a detail is missing, if fills it in with NaN

    Args:
    -----
    details : list
        List of player details
    info_dict : list
        Dictionary with player information

    Returns:
    --------
        info_dict : dict
    """

    # Add player details available
    for index, detail in enumerate(details, start=0):
        if detail == 'Club:':
            info_dict['Club'].append(details[index + 1])
        if detail == 'Nationality:':
            info_dict['Nationality'].append(details[index + 1])
        if detail == 'Age:':
            info_dict['Age'].append(details[index + 1])
        if detail == 'Position:':
            info_dict['Position'].append(details[index + 1])
        if detail == 'Prefered foot:':
            info_dict['Prefered Foot'].append(details[index + 1])
        if detail == 'Height:':
            info_dict['Height'].append(details[index + 1])
        if detail == 'Weight:':
            info_dict['Weight'].append(details[index + 1])
        if detail == 'Previous teams:':
            info_dict['Previous Teams'].append(details[index + 1])

    # Update `CreatedAt` and `UpdatedAt`
    cur_datetime = datetime.utcnow()
    info_dict['CreatedAt'].append(cur_datetime)
    info_dict['UpdatedAt'].append(cur_datetime)

    # Fill in player details missing
    return fill_missing_attribute_with_nan(info_dict)

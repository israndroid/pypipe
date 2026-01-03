import apache_beam as beam

def sanitize_header(header: str, to_lower_case=True) -> str:
    """
    Sanitize the header string by removing leading and trailing whitespace,
    converting to lowercase, and replacing spaces with underscores.
    
    Args:
        header (str): The header string to sanitize.
        to_lower_case (bool, optional): If True, convert the header to lowercase. Defaults to True, with False convert to UPPER_CASE, with None no apply case.
    Returns:
        str: The sanitized header string.
    """

    if to_lower_case:
        return header.strip().lower().replace(' ', '_')
    elif to_lower_case is False:
        return header.strip().upper().replace(' ', '_')
    elif to_lower_case is None:
        return header.strip().replace(' ', '_')
    else:
        raise ValueError("Invalid value for 'lower' parameter. Use True, False, or None.")


def split_raw_line_to_dict(row, header=None, delimiter='|', to_sanitize_header=True, to_lower_case=True)->dict:
    """_summary_
    Split a raw line into a dictionary based on the provided header (string) and delimiter.
    Args:
        row (_type_): _description_
        header (_type_, optional): _description_. Defaults to None.
        delimiter (str, optional): _description_. Defaults to '|'.

    Returns:
        dict: _description_
    """    
    splited_row = row.split(delimiter)
    if header is None:
         return {f'col_{i}': value_col.strip() for i, value_col in enumerate(splited_row)}
    elif to_sanitize_header:
        splited_header = sanitize_header(header, to_lower_case).split(delimiter)
        return {splited_header[i]: value_col.strip() for i, value_col in enumerate(splited_row)}
    else:
        splited_header = header.split(delimiter)
        return {splited_header[i]: value_col.strip() for i, value_col in enumerate(splited_row)}

def upper_case_dict_keys(row):
    """
    Convert all keys in a dictionary to uppercase.
    
    Args:
        row (dict): A dictionary with string keys.
    
    Returns:
        dict: A new dictionary with all keys converted to uppercase.
    """
    return {k.upper(): v for k, v in row.items()}

def lower_case_dict_keys(row):
    """
    Convert all keys in a dictionary to lowercase.
    
    Args:
        row (dict): A dictionary with string keys.
    
    Returns:
        dict: A new dictionary with all keys converted to lowercase.
    """
    return {k.lower(): v for k, v in row.items()}

class SplitRawLineToDict(beam.PTransform):
    """
    A Beam PTransform that splits a raw line into a dictionary based on the provided header and delimiter.
    This transform is used to standardize the format of the data in the PCollection.
    """
    def __init__(self, header=None, delimiter='|', to_sanitize_header=True, to_lower_case=True, tag='SplitRawLineToDict'):
        super().__init__()
        self.header = header
        self.delimiter = delimiter
        self.to_sanitize_header = to_sanitize_header
        self.to_lower_case = to_lower_case
        self.tag = tag
    
    def expand(self, pcoll):
        return (pcoll
                | f'{self.tag} - Split Raw Line to Dict' >> beam.Map(lambda row: split_raw_line_to_dict(row, self.header, self.delimiter, self.to_sanitize_header, self.to_lower_case)))

# def registred_functions(arg):
#     switcher = {
#         'can': lambda: get_can(table_name, filename, arg),
#         'aus': lambda: get_aus(table_name, filename, arg),
#     }
#     return switcher.get(arg, lambda: "Invalid arg")()

class SelectRenamedCreateIfNotExists(beam.PTransform):
    """
    A Beam PTransform that selects and/or renames fields in a PCollection.
    This transform is used to create a new PCollection with selected and renamed fields with default Values.
    """
    def __init__(self, field_mapping:dict, tag='SelectRenamedCreateIfNotExists'):
        super().__init__()
        self.field_mapping = field_mapping
        self.tag = tag
    
    def expand(self, pcoll):
        return (pcoll
                | 'Select and Rename Fields' >> beam.Map(lambda x: {self.field_mapping.get(k, k): v for k, v in x.items()}))


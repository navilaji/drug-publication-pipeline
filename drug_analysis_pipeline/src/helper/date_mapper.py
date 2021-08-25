import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)

pattern_1 = "%d/%m/%Y"
pattern_2 = "%Y-%m-%d"
pattern_3 = "%d %B %Y"

class DateMapper:
    '''
    This class offers some utility date function for parsing and finding the date string patterns
    '''
    def match_pattern(self, date_str, pattern):
        '''
        Decides if a date string matches a specific pattern of not
        :param date_str: the date string
        :param pattern: the date pattern
        :return: bool
        '''
        regex = datetime.strptime
        try:
            assert regex(date_str, pattern)
            return True
        except:
            return False

    def get_datepattern(self, date_str):
        '''
        Detects the pattern of a date string
        :param date_str: the input date string
        :return: str: the pattern of the input string
        '''
        if self.match_pattern(date_str, pattern_1):
            return pattern_1
        elif self.match_pattern(date_str, pattern_2):
            return pattern_2
        elif self.match_pattern(date_str, pattern_3):
            return pattern_3
        else:
            return None

    def map_to_date(self, date_str):
        '''
        Gets a date string in one of the three supported formats and then convert it to a standard date format (yyyy-mm-dd).
        If the input does not have the suppored format, the method raises an Exception
        :param date_str: the input date string
        :return: str: the mapped date strings
        '''
        print(date_str)
        if date_str is None:
            return None
        pattern = self.get_datepattern(date_str)
        try:
            return datetime.strptime(date_str, pattern).strftime(pattern_2)
        except:
            logging.error(f"could not convert {date_str} to datetime!")
            raise Exception(f"could not convert {date_str} to datetime!")

date_mapper = DateMapper()

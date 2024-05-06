
# Utility Function for holiday_analysis.py

def get_holidays(holidays):

    dates_list = []
    holiday_list = []

    with open(str(holidays), 'r') as txt:

        sentences = txt.readlines()
        
        for sentence in sentences:

            text = sentence.split('\t')
            date = text[1]
            holiday = text[2]

            dates_list.append(date)
            holiday_list.append(holiday)

    return dates_list, holiday_list


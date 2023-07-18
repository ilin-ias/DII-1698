import pandas as pd

def checking_latest_data(random_set):
    df = pd.read_csv('Testing/Test_sample.csv', header=None)
    existing_pairs = set(tuple(row) for row in df.iloc[:, :2].values.tolist())
    existing_mediaID = set(df.iloc[:, 1].tolist())

    copy = set(existing_pairs)
    for values in random_set:
        if values not in existing_pairs:
            copy.add(values)

    if len(copy) == len(existing_pairs):
        return "No New Results to Add"

    sent_email = []
    not_sent_email = []

    with open('Testing/Test_sample.csv', mode='a') as filewriter:
        for values in copy:
            if values[1] not in existing_mediaID:
                filewriter.write("{},{}".format(values[0], values[1]))
                filewriter.write("\n") 
                sent_email.append(values)
            elif values[1] in existing_mediaID and values not in existing_pairs:
                filewriter.write("{},{}".format(values[0], values[1]))
                filewriter.write("\n") 
                not_sent_email.append(values)

    return sent_email, not_sent_email

def test_send_already_exists():
    '''
    Testing that no values are added to csv and no email are sent.
    '''
    assert "No New Results to Add" == checking_latest_data(set([(14,222), (11,222), (19,221), (21,231), (21,192)]))

def test_sent_email():
    '''
    Testing that a value was sent to email.
    '''
    df = pd.read_csv('Testing/Test_sample.csv', header=None)
    existing_pairs = set(tuple(row) for row in df.iloc[:, :2].values.tolist())

    assert (1,300) not in existing_pairs
    assert (200, 78) not in existing_pairs


    assert [(1,300), (200, 78)],[] == checking_latest_data(set([(1,300), (200, 78)]))

    checking_latest_data(set([(1,300), (200, 78)]))
    df = pd.read_csv('Testing/Test_sample.csv', header=None)
    existing_pairs = set(tuple(row) for row in df.iloc[:, :2].values.tolist())

    assert (1,300) in existing_pairs
    assert (200, 78) in existing_pairs


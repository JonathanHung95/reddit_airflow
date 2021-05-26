import csv

def write_csv(list_to_write, header, file_name):
    """
    Write a csv file from list_to_write.  This should be a list of lists.  We're writing a function like this because the
    posts and comments scrapped need to be sanitized a bit (remove "&#x200B", "\n" etc.)

    list_to_write -> List of lists of posts/comments.
    header -> Header for the csv file; should be a list.
    file_name -> Name of the file.
    return -> None.
    """

    # iterate over the list of lists
    # apply a map + lambda function to clean

    for i in range(0, len(list_to_write)):
        list_to_write[i] = list(map(lambda x: str(x).replace("\n", "").replace("&#x200B", ""), list_to_write[i]))

    # write to csv file

    with open(file_name, "w", newline = "") as file:
        writer = csv.writer(file)

        # write our headers first

        writer.writerow(header)

        # iterate to write our rows from list_to_write

        for i in range(0, len(list_to_write)):
            writer.writerow(list_to_write[i])

    return None

    
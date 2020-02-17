# Function for testing python evaluation plugin, with the result of stringification given below.
# Kept in a separate file because any change to the function (even whitespace) might cause the stringified text to change.


# noinspection PyGlobalUndefined
def func():
    global result
    global df

    result = df
    result['StateZone'] = result["State"] + result["Zone"]


STRINGIFIED = "\"from types import CodeType\\n" \
              "code=CodeType(0,0,0,3,67,b't\\\\x00a\\\\x01t\\\\x01d\\\\x01\\\\x19\\\\x00t\\\\x01d\\\\x02\\\\x19\\\\x00\\\\x17\\\\x00t\\\\x01d\\\\x03<\\\\x00d\\\\x00S\\\\x00'," \
              "(None, 'State', 'Zone', 'StateZone'),('df', 'result'),(),'{}','func',6,b'\\\\x00\\\\x04\\\\x04\\\\x01',(),())\\n" \
              "exec(code)\\n\"".format(__file__)

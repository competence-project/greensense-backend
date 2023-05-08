def convertBytesToString(bytes):
    final_string = ""
    for byte_pos in range(0, len(bytes)):
        byte_content = chr(bytes[byte_pos])
        final_string = final_string + byte_content

    return final_string

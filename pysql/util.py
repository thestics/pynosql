

def read_lines(f, chunk_size=1024):
    # TODO: fix
    char_no = 0
    for line in f.readlines():
        yield char_no, line
        char_no += len(line)

    # data = ''
    #
    # while True:
    #     chunk = f.read(chunk_size)
    #     if not chunk:
    #         return
    #
    #     data += chunk
    #     cr_index = data.find('\n')
    #     if cr_index != -1:
    #         yield data[:cr_index]
    #         data = data[cr_index:]
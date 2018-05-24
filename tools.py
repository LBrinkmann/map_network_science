import jsonlines


### io ###

def write_json_lines(stream, filename):
    with jsonlines.open(filename, mode='w', flush=True) as writer:
        for obj in stream:
            writer.write(obj)

def load_json_lines(filename):
    with jsonlines.open(filename) as reader:
        for obj in reader:
            yield reader.read()
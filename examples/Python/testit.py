import os

CHUNK_SIZE = 250000

def main():
    with open("testdata", "rb") as file:
        offset = 0

        while True:
            file.seek(offset, os.SEEK_SET)
            if data := file.read(CHUNK_SIZE):
                offset += len(data)

            else:
                break

if __name__ == '__main__':
    main()

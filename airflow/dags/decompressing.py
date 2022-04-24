#import zstandard
import pathlib
import os
import tarfile
import logging

def decompressing_callable():
    parent_dir=os.getcwd()
    directory="data"
    path = os.path.join(parent_dir, directory)
    inputfile_zst=os.path.join(path, "2022-04-17_23-56.tar.zst")
    #destination_dir_zst=path
    inputfile_tar=os.path.join(path, "2022-04-17_23-56.tar")
    destination_dir_tar=path
    
    #logging.info("decompressing zst")
    #decompress_zstandard_to_folder(inputfile_zst,destination_dir_zst)
    logging.info("decompressing tar")
    decompress_tar_to_folder(inputfile_tar,destination_dir_tar)
    logging.info("complete")
    logging.info("removing zst file")
    os.remove(inputfile_zst)
    logging.info("removing tar file")
    os.remove(inputfile_tar)

#def decompress_zstandard_to_folder(input_file,destination_dir):
#    input_file = pathlib.Path(input_file)
#    with open(input_file, 'rb') as compressed:
#        decomp = zstandard.ZstdDecompressor()
#        output_path = pathlib.Path(destination_dir) / input_file.stem
#        with open(output_path, 'wb') as destination:
#            decomp.copy_stream(compressed, destination)

def decompress_tar_to_folder(input_file,destination_dir):
    tar_file = tarfile.open(input_file)
    tar_file.extractall(destination_dir) # specify which folder to extract to
    tar_file.close()

def main():
    ''' Function to scrape sponsorblock data from a daily updated mirror database.
    '''
    decompressing_callable()

if __name__ == '__main__':
    main()
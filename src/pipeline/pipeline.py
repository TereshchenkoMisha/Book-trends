import llm_processor_ver2 as llm
import genres_processing as genres
import cleaner

def main():
    llm.main()
    genres.main()
    cleaner.main()

if __name__ == "__main__":
    main()
from src.pipeline.scraper_gr import gr_scraper_main

def main():
    print("Start")
    try:
        gr_scraper_main()
    except KeyboardInterrupt:
        print("\n Scraper stopped by user.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
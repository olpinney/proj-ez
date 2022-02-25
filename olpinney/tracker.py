''' tracker.py

This is an application module that allows a user to find information about
specific routes.
'''
import fare_crawler
import sys

MENU = '''
********* Route Tracker *********
Welcome to the route tracker application! Please choose
an option to perform a task.

(1) List all route information
(2) Search a for a specific route
(3) Quit the program
'''
START = 1
END = 3


def find_route():
    print("\n---Find Route---")
    from_city = input("Enter in the from city: ")
    to_city = input("Enter in the to city: ")
    df = fare_crawler.crawl([to_city])
    mask = (df["from_city"] == from_city) & (
        df["to_city"] == to_city)
    route = df[mask]
    if len(route) == 1:
        print(
            f'From:{from_city} To:{to_city} -> Lowest Price: ${route.iloc[0]["fare"]:.2f}')
    else:
        print(
            f"ERROR: Could not find route information for {from_city} -> {to_city}")


def retrieve_task():
    option = -1
    while True:
        print(MENU)
        option = int(input("Option: "))
        if option >= START and option <= END:
            break
        else:
            print(f"Invalid option({option})")
    return option


OPTIONS_HANDLER = {
    1: lambda: print(f"\n---All Route Information---\n{fare_crawler.crawl()}"),
    2: lambda: find_route()
}


def main():
    while True:
        option = retrieve_task()
        if option == 3:
            break
        else:
            handler = OPTIONS_HANDLER[option]
            handler()


if __name__ == "__main__":
    # This is the entry point into the application
    main()

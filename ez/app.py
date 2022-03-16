from link_records import record_link
from map import server


def print_results():
    '''
    '''

def run(sys_arg):
    """
    """
    if sys_arg == "run":
        record_link.go()
    elif sys_arg == "map":
        server.create_map()



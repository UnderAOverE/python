import argparse

def parse_arguments():
    """
    Parses command-line arguments for environment, sectors, regions, lobs,
    domains, businessgroups, and applications.

    Returns:
        argparse.Namespace: An object containing the parsed arguments as attributes.

    Raises:
        argparse.ArgumentTypeError: If applications are provided but sectors are missing.
    """

    parser = argparse.ArgumentParser(description="Process arguments with default 'global'.")

    parser.add_argument(
        "-es",
        "--environments",
        nargs="+",
        default=["global"],
        help="List of environments (default: global)",
    )
    parser.add_argument(
        "-ss",
        "--sectors",
        nargs="+",
        default=["global"],
        help="List of sectors (default: global).  Use quotes to enclose sectors with spaces (e.g., 'Bus Sales')",
    )
    parser.add_argument(
        "-rs",
        "--regions",
        nargs="+",
        default=["global"],
        help="List of regions (default: global)",
    )
    parser.add_argument(
        "-ls",
        "--lobs",
        nargs="+",
        default=["global"],
        help="List of lines of business (default: global)",
    )
    parser.add_argument(
        "-ds",
        "--domains",
        nargs="+",
        default=["global"],
        help="List of domains (default: global)",
    )
    parser.add_argument(
        "-bgs",
        "--businessgroups",
        nargs="+",
        default=["global"],
        help="List of business groups (default: global)",
    )
    parser.add_argument(
        "-as",
        "--applications",
        nargs="+",
        default=["global"],
        help="List of applications (default: global)",
    )

    args = parser.parse_args()

    if args.applications != ["global"] and args.sectors == ["global"]:
        raise argparse.ArgumentTypeError("If applications are provided, sectors are required.")

    return args


def main(environments, sectors, regions, lobs, domains, businessgroups, applications):
    """
    Main function to process the parsed arguments.

    Args:
        environments (list): List of environments.
        sectors (list): List of sectors.
        regions (list): List of regions.
        lobs (list): List of lines of business.
        domains (list): List of domains.
        businessgroups (list): List of business groups.
        applications (list): List of applications.
    """
    print(f"Environments: {environments}")
    print(f"Sectors: {sectors}")
    print(f"Regions: {regions}")
    print(f"LoBS: {lobs}")
    print(f"Domains: {domains}")
    print(f"Business Groups: {businessgroups}")
    print(f"Applications: {applications}")


if __name__ == "__main__":
    try:
        args = parse_arguments()
        main(
            args.environments,
            args.sectors,
            args.regions,
            args.lobs,
            args.domains,
            args.businessgroups,
            args.applications,
        )
    except argparse.ArgumentTypeError as e:
        print(f"Error: {e}")
    except SystemExit:
        # Handles argparse's automatic exit on --help or invalid arguments
        pass

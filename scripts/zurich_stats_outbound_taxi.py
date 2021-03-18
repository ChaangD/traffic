import click


def pp(flight):
    parking = flight.parking_position("LSZH")
    if parking is None:
        return flight
    return flight.assign(
        parking_position=parking.parking_position_max,
        parking_start=parking.start,
        parking_stop=parking.stop,
    )


@click.command()
@click.argument("filename")
@click.option("--max_number_flights", type=int, default=-1)
@click.option("-o", "--output_file")
def main(filename, max_number_flights, output_file):
    from traffic.core import Traffic

    t_ = Traffic.from_file("taxi_zurich_2019_takeoff.pkl")
    assert t_ is not None

    toto = t_.iterate_lazy().pipe(pp).eval(desc="", max_workers=8)

    temp = toto.summary(
        [
            "flight_id",
            "callsign",
            "icao24",
            "parking_position_max",
            "parking_start_max",
            "parking_stop_max",
        ]
    )

    temp.query("parking_position_max == parking_position_max").sort_values(
        "parking_start_max"
    ).to_pickle(output_file)


if __name__ == "__main__":
    main()

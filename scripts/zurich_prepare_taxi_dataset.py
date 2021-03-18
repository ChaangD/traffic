import sys
from pathlib import Path

import pandas as pd
from traffic.core import Traffic
from traffic.data import airports


def clip_distance(flight):
    lat_only = flight.resample("1S").query("latitude == latitude")
    if lat_only is None:
        return None
    distance_4 = lat_only.distance(airports["LSZH"]).query("distance < 4")
    if distance_4 is None:
        return None

    return flight.before(distance_4.stop)


if __name__ == "__main__":

    cumul = list()
    for file in Path(sys.argv[1]).glob("*_history.pkl.gz"):
        t = Traffic.from_file(file)
        assert t is not None
        t_ = (
            t.query('origin== "LSZH"')  # type: ignore
            .iterate_lazy()
            .pipe(clip_distance)
            .eval(desc=file.stem)
        )
        cumul.append(t_)

    t_ = Traffic(pd.concat([x.data for x in cumul]).sort_values("timestamp"))
    t_.assign_id().eval(desc="").to_pickle("taxi_zurich_2019_takeoff.pkl")

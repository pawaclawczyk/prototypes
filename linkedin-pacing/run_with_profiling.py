import cProfile
import sys
from datetime import datetime

from scenario import Scenario

if __name__ == "__main__":
    prof = cProfile.Profile()
    scn = Scenario("profile", "data", 1_000_000, 10, 1.0)
    prof.enable()
    scn.run()
    prof.disable()
    prof_name = sys.argv[1] if len(sys.argv) > 1 else datetime.utcnow().isoformat()
    prof.dump_stats(prof_name + ".prof")

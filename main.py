import configuration
import scenario

if __name__ == "__main__":
    for configuration in configuration.SCENARIOS:
        scenario.Scenario(**configuration).run()

from pathlib import Path

WINDOWS = 24 * 60
AVG_REQUESTS_PER_WINDOW = 60 * 100
NUMBER_OF_REQUESTS = WINDOWS * AVG_REQUESTS_PER_WINDOW

NUMBER_OF_CAMPAIGNS_LOW = 10
NUMBER_OF_CAMPAIGNS_HIGH = 100

BUDGET_TO_REQUESTS_RATE_BELOW = 0.8
BUDGET_TO_REQUESTS_RATE_EXACT = 1.0
BUDGET_TO_REQUESTS_RATE_ABOVE = 1.2

OUTPUT_DIR = Path("data")

SCENARIOS = [
    # {
    #     "name": "low_competition_over_delivery",
    #     "output_dir": OUTPUT_DIR,
    #     "number_of_requests": NUMBER_OF_REQUESTS,
    #     "number_of_campaigns": NUMBER_OF_CAMPAIGNS_LOW,
    #     "budget_to_requests_rate": BUDGET_TO_REQUESTS_RATE_BELOW,
    # },
    # {
    #     "name": "low_competition_exact_delivery",
    #     "output_dir": OUTPUT_DIR,
    #     "number_of_requests": NUMBER_OF_REQUESTS,
    #     "number_of_campaigns": NUMBER_OF_CAMPAIGNS_LOW,
    #     "budget_to_requests_rate": BUDGET_TO_REQUESTS_RATE_EXACT,
    # },
    {
        "name": "low_competition_under_delivery",
        "output_dir": OUTPUT_DIR,
        "number_of_requests": NUMBER_OF_REQUESTS,
        "number_of_campaigns": NUMBER_OF_CAMPAIGNS_LOW,
        "budget_to_requests_rate": BUDGET_TO_REQUESTS_RATE_ABOVE,
    },
    # {
    #     "name": "high_competition_over_delivery",
    #     "output_dir": OUTPUT_DIR,
    #     "number_of_requests": NUMBER_OF_REQUESTS,
    #     "number_of_campaigns": NUMBER_OF_CAMPAIGNS_HIGH,
    #     "budget_to_requests_rate": BUDGET_TO_REQUESTS_RATE_BELOW,
    # },
    # {
    #     "name": "high_competition_exact_delivery",
    #     "output_dir": OUTPUT_DIR,
    #     "number_of_requests": NUMBER_OF_REQUESTS,
    #     "number_of_campaigns": NUMBER_OF_CAMPAIGNS_HIGH,
    #     "budget_to_requests_rate": BUDGET_TO_REQUESTS_RATE_EXACT,
    # },
    # {
    #     "name": "high_competition_under_delivery",
    #     "output_dir": OUTPUT_DIR,
    #     "number_of_requests": NUMBER_OF_REQUESTS,
    #     "number_of_campaigns": NUMBER_OF_CAMPAIGNS_HIGH,
    #     "budget_to_requests_rate": BUDGET_TO_REQUESTS_RATE_ABOVE,
    # },
]

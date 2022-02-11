import numpy as np
from scipy.stats import norm, uniform


def normalize(arr: np.array) -> np.array:
    return arr / arr.sum()


def uniform_dist(ticks):
    points = np.linspace(uniform.ppf(0.01), uniform.ppf(0.99), ticks)
    return normalize(uniform.pdf(points))


def normal_dist(ticks, loc=0.0, scale=1.0):
    points = np.linspace(norm.ppf(0.001), norm.ppf(0.999), ticks)
    return normalize(norm.pdf(points, loc=loc, scale=scale))


def custom_dist(ticks):
    a = normal_dist(ticks, -1, 0.5) * 0.75
    b = normal_dist(ticks, 1.5, 0.5)
    return normalize(a + b)


def traffic_distribution(dist, traffic):
    return (normalize(dist) * traffic).round().astype(int)

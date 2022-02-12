import numpy as np
from scipy.stats import norm, uniform


def normalize(arr: np.ndarray) -> np.ndarray:
    return arr / arr.sum()


def uniform_dist(n: int):
    points = np.linspace(uniform.ppf(0.01), uniform.ppf(0.99), n)
    return normalize(uniform.pdf(points))


def normal_dist(n: int, loc: float = 0.0, scale: float = 1.0):
    points = np.linspace(norm.ppf(0.01), norm.ppf(0.99), n)
    return normalize(norm.pdf(points, loc=loc, scale=scale))


def custom_dist(n: int):
    a = normal_dist(n, -1, 0.5) * 0.75
    b = normal_dist(n, 1.5, 0.5)
    return normalize(a + b)


def traffic_dist(dist: np.ndarray, reqs: int) -> np.ndarray:
    return (normalize(dist) * reqs).round().astype(int)

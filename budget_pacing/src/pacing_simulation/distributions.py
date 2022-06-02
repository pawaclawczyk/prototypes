import numpy as np
from scipy.stats import norm, uniform


def normalize(arr: np.ndarray) -> np.ndarray:
    """Normalizes values to range [0.0, 1.0]"""
    return arr / arr.sum()


def uniform_distribution(n: int) -> np.ndarray:
    """Discrete uniform probability distribution."""
    points = np.linspace(uniform.ppf(0.01), uniform.ppf(0.99), n)
    return normalize(uniform.pdf(points))


def normal_dist(n: int, loc: float = 0.0, scale: float = 1.0) -> np.ndarray:
    """Discrete normal probability distribution."""
    points = np.linspace(norm.ppf(0.01), norm.ppf(0.99), n)
    return normalize(norm.pdf(points, loc=loc, scale=scale))


def custom_distribution(n: int) -> np.ndarray:
    a = normal_dist(n, -1, 0.5) * 0.75
    b = normal_dist(n, 1.5, 0.5)
    return normalize(a + b)


def traffic_distribution(distribution: np.ndarray, requests: int) -> np.ndarray:
    """Converts discrete probability distribution to traffic distribution of given number of requests."""
    return (normalize(distribution) * requests).round().astype(int)

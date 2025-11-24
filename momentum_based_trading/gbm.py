import numpy as np
from scipy.stats import norm


class GBM:
    def __init__(self) -> None:
        self.mu: float = np.nan
        self.sigma: float = np.nan
        self.rng = np.random.default_rng()

    def simulate(self, n: int, k: int, dt: float, s0: float) -> np.ndarray:
        sqrt_dt = 1 / np.sqrt(n)
        traj = np.full((n + 1, k), np.nan)
        drift = (self.mu - self.sigma**2 / 2) * np.linspace(1, n, n) * dt
        for i in range(k):
            W = sqrt_dt * np.cumsum(norm.rvs(size=n))
            traj[1:, i] = s0 * np.exp(drift + self.sigma * W)
            traj[0, i] = s0
        return traj

    def calibrate(self, trajectory: np.ndarray, dt: float) -> None:
        increments = np.diff(np.log(trajectory))
        moments = [0, 0]
        n_iter = 10
        for _ in range(n_iter):
            X = self.rng.choice(increments, size=len(increments) // 2)
            moments[0] += np.mean(X) / n_iter
            moments[1] += np.mean(X**2) / n_iter
        std = np.sqrt(moments[1] - moments[0] ** 2)
        self.sigma = std / np.sqrt(dt)
        self.mu = moments[0] / dt + self.sigma**2 / 2

    def forecast(
        self, latest: float, t: float, confidence: float
    ) -> dict[str, np.ndarray]:
        mu = (self.mu - self.sigma**2 / 2) * t
        s: float = self.sigma * np.sqrt(t)
        q = norm.ppf([1 - confidence / 2, 1 + confidence / 2], loc=mu, scale=s)
        return {
            "expected": latest * np.exp(self.mu * t),
            "interval": latest * np.exp(q),
        }

    def expected_shortfall(self, t: float, confidence: float) -> float:
        m = (self.mu - self.sigma**2 / 2) * t
        s: float = self.sigma * np.sqrt(t)
        es = -m + s * norm.pdf(norm.ppf(confidence)) / (1 - confidence)
        return es

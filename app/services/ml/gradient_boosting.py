"""
Pure Python Gradient Boosting Classifier

ULTRA Follow-Up System V2.0 - Sprint 2

A lightweight gradient boosting implementation for binary classification
that doesn't require scikit-learn or other external ML libraries.

Based on the GBDT algorithm:
1. Start with initial prediction (log odds of positive class)
2. For each iteration:
   a. Compute residuals (gradients)
   b. Fit a decision tree to residuals
   c. Update predictions with learning rate
3. Final prediction is sum of all tree predictions
"""

from typing import List, Dict, Optional, Tuple, Any
from dataclasses import dataclass, field
from collections import defaultdict
import math
import random
import json
import logging

logger = logging.getLogger(__name__)


@dataclass
class DecisionNode:
    """
    A node in a decision tree.

    For internal nodes: feature_idx and threshold define the split
    For leaf nodes: is_leaf=True and value contains the prediction
    """
    feature_idx: int = 0
    threshold: float = 0.0
    value: float = 0.0  # Leaf value (residual prediction)
    left: Optional['DecisionNode'] = None
    right: Optional['DecisionNode'] = None
    is_leaf: bool = True
    n_samples: int = 0

    def predict(self, x: List[float]) -> float:
        """Traverse tree to get prediction for a single sample"""
        if self.is_leaf:
            return self.value

        if x[self.feature_idx] <= self.threshold:
            return self.left.predict(x) if self.left else self.value
        else:
            return self.right.predict(x) if self.right else self.value

    def to_dict(self) -> Dict[str, Any]:
        """Convert node to dictionary for serialization"""
        result = {
            "feature_idx": self.feature_idx,
            "threshold": self.threshold,
            "value": self.value,
            "is_leaf": self.is_leaf,
            "n_samples": self.n_samples,
        }
        if self.left:
            result["left"] = self.left.to_dict()
        if self.right:
            result["right"] = self.right.to_dict()
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DecisionNode':
        """Create node from dictionary"""
        node = cls(
            feature_idx=data.get("feature_idx", 0),
            threshold=data.get("threshold", 0.0),
            value=data.get("value", 0.0),
            is_leaf=data.get("is_leaf", True),
            n_samples=data.get("n_samples", 0),
        )
        if "left" in data:
            node.left = cls.from_dict(data["left"])
        if "right" in data:
            node.right = cls.from_dict(data["right"])
        return node


class GradientBoostingClassifier:
    """
    Pure Python Gradient Boosting for Binary Classification.

    Uses decision stumps (trees with limited depth) as weak learners.
    Optimizes log-loss for probability estimation.

    Parameters:
    - n_estimators: Number of boosting iterations (trees)
    - learning_rate: Shrinkage factor for each tree's contribution
    - max_depth: Maximum depth of each tree
    - min_samples_split: Minimum samples required to split a node
    - subsample: Fraction of samples used for each tree (stochastic GB)

    Example:
        model = GradientBoostingClassifier(n_estimators=100)
        model.fit(X_train, y_train)
        prob = model.predict_proba(x_test)
    """

    def __init__(
        self,
        n_estimators: int = 100,
        learning_rate: float = 0.1,
        max_depth: int = 3,
        min_samples_split: int = 10,
        subsample: float = 0.8,
        random_state: Optional[int] = None
    ):
        self.n_estimators = n_estimators
        self.learning_rate = learning_rate
        self.max_depth = max_depth
        self.min_samples_split = min_samples_split
        self.subsample = subsample
        self.random_state = random_state

        self.trees: List[DecisionNode] = []
        self.initial_prediction: float = 0.0
        self.feature_importance: Dict[int, float] = defaultdict(float)
        self.is_fitted: bool = False
        self.n_features: int = 0

        if random_state is not None:
            random.seed(random_state)

    def fit(self, X: List[List[float]], y: List[int]) -> 'GradientBoostingClassifier':
        """
        Train the gradient boosting model.

        Args:
            X: Training features, shape (n_samples, n_features)
            y: Training labels, 0 or 1

        Returns:
            self
        """
        n_samples = len(X)
        if n_samples == 0:
            raise ValueError("Cannot fit with empty data")

        self.n_features = len(X[0])

        # Calculate initial prediction (log odds)
        pos_count = sum(y)
        neg_count = n_samples - pos_count

        if pos_count == 0 or neg_count == 0:
            self.initial_prediction = 0.0
        else:
            # Log odds of positive class
            self.initial_prediction = math.log(pos_count / neg_count)

        # Initialize predictions
        predictions = [self.initial_prediction] * n_samples

        logger.debug(
            f"[GB] Training with {n_samples} samples, {self.n_features} features, "
            f"{pos_count} positive, {neg_count} negative"
        )

        # Boosting iterations
        for iteration in range(self.n_estimators):
            # Compute probabilities and residuals
            probabilities = [self._sigmoid(p) for p in predictions]
            residuals = [y[i] - probabilities[i] for i in range(n_samples)]

            # Subsample data
            if self.subsample < 1.0:
                sample_indices = random.sample(
                    range(n_samples),
                    int(n_samples * self.subsample)
                )
            else:
                sample_indices = list(range(n_samples))

            X_sample = [X[i] for i in sample_indices]
            residuals_sample = [residuals[i] for i in sample_indices]

            # Fit tree to residuals
            tree = self._fit_tree(X_sample, residuals_sample, depth=0)
            self.trees.append(tree)

            # Update predictions
            for i in range(n_samples):
                tree_pred = tree.predict(X[i])
                predictions[i] += self.learning_rate * tree_pred

            # Log progress every 20 iterations
            if (iteration + 1) % 20 == 0:
                loss = self._compute_loss(predictions, y)
                logger.debug(f"[GB] Iteration {iteration + 1}/{self.n_estimators}, loss: {loss:.4f}")

        self.is_fitted = True
        logger.info(f"[GB] Training complete. {len(self.trees)} trees fitted.")

        return self

    def _fit_tree(
        self,
        X: List[List[float]],
        residuals: List[float],
        depth: int
    ) -> DecisionNode:
        """
        Recursively fit a decision tree to residuals.

        Args:
            X: Feature matrix
            residuals: Target residuals
            depth: Current depth in tree

        Returns:
            Root node of fitted tree
        """
        n_samples = len(X)

        # Create leaf node if stopping conditions met
        if (depth >= self.max_depth or
            n_samples < self.min_samples_split or
            n_samples < 2):
            # Leaf value: mean of residuals (for log-loss, this is the optimal value)
            leaf_value = sum(residuals) / n_samples if n_samples > 0 else 0.0
            return DecisionNode(
                is_leaf=True,
                value=leaf_value,
                n_samples=n_samples
            )

        # Find best split
        best_feature, best_threshold, best_gain = self._find_best_split(X, residuals)

        if best_gain <= 0:
            # No valid split found
            leaf_value = sum(residuals) / n_samples if n_samples > 0 else 0.0
            return DecisionNode(
                is_leaf=True,
                value=leaf_value,
                n_samples=n_samples
            )

        # Update feature importance
        self.feature_importance[best_feature] += best_gain

        # Split data
        left_X, left_residuals = [], []
        right_X, right_residuals = [], []

        for i in range(n_samples):
            if X[i][best_feature] <= best_threshold:
                left_X.append(X[i])
                left_residuals.append(residuals[i])
            else:
                right_X.append(X[i])
                right_residuals.append(residuals[i])

        # Recursively build subtrees
        left_node = self._fit_tree(left_X, left_residuals, depth + 1)
        right_node = self._fit_tree(right_X, right_residuals, depth + 1)

        return DecisionNode(
            feature_idx=best_feature,
            threshold=best_threshold,
            is_leaf=False,
            left=left_node,
            right=right_node,
            n_samples=n_samples
        )

    def _find_best_split(
        self,
        X: List[List[float]],
        residuals: List[float]
    ) -> Tuple[int, float, float]:
        """
        Find the best feature and threshold to split on.

        Uses variance reduction as split criterion.

        Returns:
            (feature_index, threshold, gain)
        """
        n_samples = len(X)
        n_features = len(X[0]) if X else 0

        best_feature = 0
        best_threshold = 0.0
        best_gain = -float('inf')

        # Total variance
        total_mean = sum(residuals) / n_samples if n_samples > 0 else 0
        total_variance = sum((r - total_mean) ** 2 for r in residuals)

        for feature_idx in range(n_features):
            # Get unique values for this feature
            values = sorted(set(x[feature_idx] for x in X))

            if len(values) <= 1:
                continue

            # Try splits between consecutive values
            for i in range(len(values) - 1):
                threshold = (values[i] + values[i + 1]) / 2

                # Split data
                left_residuals = [residuals[j] for j in range(n_samples)
                                 if X[j][feature_idx] <= threshold]
                right_residuals = [residuals[j] for j in range(n_samples)
                                  if X[j][feature_idx] > threshold]

                if len(left_residuals) < 2 or len(right_residuals) < 2:
                    continue

                # Calculate variance reduction
                left_mean = sum(left_residuals) / len(left_residuals)
                right_mean = sum(right_residuals) / len(right_residuals)

                left_var = sum((r - left_mean) ** 2 for r in left_residuals)
                right_var = sum((r - right_mean) ** 2 for r in right_residuals)

                # Gain = reduction in variance
                gain = total_variance - left_var - right_var

                if gain > best_gain:
                    best_gain = gain
                    best_feature = feature_idx
                    best_threshold = threshold

        return best_feature, best_threshold, best_gain

    def predict_proba(self, x: List[float]) -> float:
        """
        Predict probability for a single sample.

        Args:
            x: Feature vector

        Returns:
            Probability of positive class (0-1)
        """
        if not self.is_fitted:
            raise RuntimeError("Model not fitted. Call fit() first.")

        # Sum initial prediction and all tree predictions
        raw_prediction = self.initial_prediction

        for tree in self.trees:
            raw_prediction += self.learning_rate * tree.predict(x)

        # Convert to probability
        return self._sigmoid(raw_prediction)

    def predict_proba_batch(self, X: List[List[float]]) -> List[float]:
        """Predict probabilities for multiple samples"""
        return [self.predict_proba(x) for x in X]

    def predict(self, x: List[float], threshold: float = 0.5) -> int:
        """
        Make binary prediction for a single sample.

        Args:
            x: Feature vector
            threshold: Classification threshold

        Returns:
            0 or 1
        """
        return 1 if self.predict_proba(x) >= threshold else 0

    def predict_batch(self, X: List[List[float]], threshold: float = 0.5) -> List[int]:
        """Make binary predictions for multiple samples"""
        return [self.predict(x, threshold) for x in X]

    def _sigmoid(self, x: float) -> float:
        """Sigmoid function with numerical stability"""
        if x >= 0:
            return 1 / (1 + math.exp(-x))
        else:
            exp_x = math.exp(x)
            return exp_x / (1 + exp_x)

    def _compute_loss(self, predictions: List[float], y: List[int]) -> float:
        """Compute binary cross-entropy loss"""
        eps = 1e-15
        loss = 0.0

        for i, pred in enumerate(predictions):
            prob = self._sigmoid(pred)
            prob = max(eps, min(1 - eps, prob))  # Clip for numerical stability
            loss += -y[i] * math.log(prob) - (1 - y[i]) * math.log(1 - prob)

        return loss / len(predictions) if predictions else 0.0

    def get_feature_importance(self, normalize: bool = True) -> Dict[int, float]:
        """
        Get feature importance scores.

        Args:
            normalize: If True, normalize to sum to 1

        Returns:
            Dict mapping feature index to importance score
        """
        importance = dict(self.feature_importance)

        if normalize and importance:
            total = sum(importance.values())
            if total > 0:
                importance = {k: v / total for k, v in importance.items()}

        return importance

    def export_model(self) -> Dict[str, Any]:
        """
        Export model to dictionary for persistence.

        Returns:
            Dictionary that can be serialized to JSON
        """
        return {
            "version": "1.0.0",
            "n_estimators": self.n_estimators,
            "learning_rate": self.learning_rate,
            "max_depth": self.max_depth,
            "min_samples_split": self.min_samples_split,
            "subsample": self.subsample,
            "initial_prediction": self.initial_prediction,
            "n_features": self.n_features,
            "feature_importance": dict(self.feature_importance),
            "trees": [tree.to_dict() for tree in self.trees],
        }

    def import_model(self, data: Dict[str, Any]) -> 'GradientBoostingClassifier':
        """
        Import model from dictionary.

        Args:
            data: Dictionary from export_model()

        Returns:
            self
        """
        self.n_estimators = data.get("n_estimators", 100)
        self.learning_rate = data.get("learning_rate", 0.1)
        self.max_depth = data.get("max_depth", 3)
        self.min_samples_split = data.get("min_samples_split", 10)
        self.subsample = data.get("subsample", 0.8)
        self.initial_prediction = data.get("initial_prediction", 0.0)
        self.n_features = data.get("n_features", 0)
        self.feature_importance = defaultdict(float, data.get("feature_importance", {}))

        self.trees = [
            DecisionNode.from_dict(tree_data)
            for tree_data in data.get("trees", [])
        ]

        self.is_fitted = len(self.trees) > 0

        return self

    def save_to_json(self, filepath: str) -> None:
        """Save model to JSON file"""
        with open(filepath, 'w') as f:
            json.dump(self.export_model(), f, indent=2)

    @classmethod
    def load_from_json(cls, filepath: str) -> 'GradientBoostingClassifier':
        """Load model from JSON file"""
        with open(filepath, 'r') as f:
            data = json.load(f)

        model = cls()
        model.import_model(data)
        return model

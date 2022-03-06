# crypto_portfolio_bot

メモ : 負のポジションを許し、ポジションの絶対値の合計を1以下にするためにpyportfoliooptの_make_weight_sum_constraint関数に変更を加えないと動かない
```python
def _make_weight_sum_constraint(self, is_market_neutral):
    ...
    else:
        # Check negative bound
        negative_possible = np.any(self._lower_bounds < 0)
        if negative_possible:
            # Use norm1 as position constraint
            self.add_constraint(lambda w: cp.sum(cp.abs(w)) <= 1)
        else: 
            self.add_constraint(lambda w: cp.sum(w) == 1)
    self._market_neutral = is_market_neutral```
'''
"""
PyTorch anomaly detection model.

Architecture: Lightweight attention-based network that processes
user transaction feature vectors and outputs anomaly probability.
"""

import torch
import torch.nn as nn


class TransactionAnomalyDetector(nn.Module):
    """
    Multi-head self-attention anomaly detector.

    Takes a feature vector of user+transaction features and
    outputs P(anomaly). Designed for low-latency inference.
    """

    def __init__(self, input_dim: int = 16, hidden_dim: int = 64, num_heads: int = 4, dropout: float = 0.1):
        super().__init__()
        self.input_projection = nn.Linear(input_dim, hidden_dim)
        self.attention = nn.MultiheadAttention(embed_dim=hidden_dim, num_heads=num_heads, dropout=dropout, batch_first=True)
        self.norm = nn.LayerNorm(hidden_dim)
        self.classifier = nn.Sequential(
            nn.Linear(hidden_dim, hidden_dim // 2),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim // 2, 1),
            nn.Sigmoid(),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        x = self.input_projection(x).unsqueeze(1)  # (B, 1, H)
        attn_out, _ = self.attention(x, x, x)
        x = self.norm(x + attn_out).squeeze(1)  # (B, H)
        return self.classifier(x).squeeze(-1)  # (B,)

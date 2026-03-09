# -*- coding: utf-8 -*-
"""
Embedding model manager - local HuggingFace model, no API needed.
Reference: wikiqa_rag/model_manager.py
"""
import os
import logging

# 在导入 TensorFlow 前设置，抑制 oneDNN 和 TF 警告
os.environ.setdefault("TF_ENABLE_ONEDNN_OPTS", "0")
os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "3")  # 仅 ERROR

# 抑制 TF 的 Python 日志
logging.getLogger("tensorflow").setLevel(logging.ERROR)
logging.getLogger("tf_keras").setLevel(logging.ERROR)

_embedding_model = None


def get_embedding_model(model_name: str = None):
    """Get embedding model singleton. Use BGE Chinese model for Chinese docs."""
    global _embedding_model
    if _embedding_model is not None:
        return _embedding_model
    try:
        from langchain_huggingface import HuggingFaceEmbeddings
        from config import EMBEDDING_MODEL
        name = model_name or EMBEDDING_MODEL
        _embedding_model = HuggingFaceEmbeddings(
            model_name=name,
            model_kwargs={"device": "cpu"},
            encode_kwargs={"normalize_embeddings": True},
        )
        return _embedding_model
    except ImportError:
        try:
            from sentence_transformers import SentenceTransformer
            from config import EMBEDDING_MODEL
            name = model_name or EMBEDDING_MODEL
            _embedding_model = SentenceTransformer(name)
            return _embedding_model
        except ImportError:
            raise ImportError(
                "pip install langchain-huggingface sentence-transformers"
            )

"""
Text2SQL适配器 - 统一接口支持多种实现方案
"""
from typing import Dict, Any
from database import DatabaseManager
import importlib.util


class Text2SQLAdapter:
    """Text2SQL适配器，支持多种实现方案切换"""
    
    def __init__(self, db_manager: DatabaseManager, scheme: str = "original"):
        """
        初始化适配器
        
        Args:
            db_manager: 数据库管理器
            scheme: 使用的方案 ("original", "langchain", "enhanced")
        """
        self.db_manager = db_manager
        self.scheme = scheme
        self._instance = None
        self._initialize_instance()
    
    def _initialize_instance(self):
        """根据方案初始化对应的实例"""
        if self.scheme == "original":
            from text2sql import Text2SQL
            self._instance = Text2SQL(self.db_manager)
            self.name = "原始实现"
            self.description = "基于OpenAI API的直接实现，已优化模糊匹配"
            self.features = [
                "✅ 快速响应",
                "✅ 成本较低",
                "✅ 已优化年份/日期模糊匹配",
                "✅ 输入预处理"
            ]
        
        elif self.scheme == "langchain":
            try:
                from text2sql_langchain import Text2SQLLangChain
                self._instance = Text2SQLLangChain(self.db_manager)
                self.name = "LangChain增强版"
                self.description = "基于LangChain SQL Agent，功能更强大"
                self.features = [
                    "✅ 自动错误处理",
                    "✅ 查询验证",
                    "✅ 支持复杂查询",
                    "✅ 多轮对话支持"
                ]
            except ImportError as e:
                raise ImportError(
                    f"LangChain方案需要额外依赖: {e}\n"
                    "请运行: pip install langchain-community"
                )
        
        elif self.scheme == "enhanced":
            from text2sql_enhanced import Text2SQLEnhanced
            # 使用增强的提示词和更多示例
            self._instance = Text2SQLEnhanced(self.db_manager)
            self.name = "增强优化版"
            self.description = "原始实现的增强版本，使用更多Few-shot示例"
            self.features = [
                "✅ 更多Few-shot示例（12个）",
                "✅ 优化的提示词",
                "✅ 智能预处理",
                "✅ 更好的模糊匹配"
            ]
        else:
            raise ValueError(f"未知的方案: {self.scheme}")
    
    def query(self, question: str) -> Dict[str, Any]:
        """
        执行查询（统一接口）
        
        Args:
            question: 自然语言问题
        
        Returns:
            查询结果字典
        """
        return self._instance.query(question)
    
    def switch_scheme(self, new_scheme: str):
        """切换方案"""
        if new_scheme != self.scheme:
            self.scheme = new_scheme
            self._initialize_instance()
    
    def get_info(self) -> Dict[str, Any]:
        """获取当前方案信息"""
        return {
            'scheme': self.scheme,
            'name': self.name,
            'description': self.description,
            'features': self.features
        }


def get_available_schemes() -> Dict[str, Dict[str, str]]:
    """获取所有可用的方案"""
    schemes = {
        'original': {
            'name': '原始实现',
            'description': '基于OpenAI API的直接实现',
            'icon': '⚡'
        },
        'enhanced': {
            'name': '增强优化版',
            'description': '更多Few-shot示例和优化提示词',
            'icon': '🚀'
        },
        'langchain': {
            'name': 'LangChain增强版',
            'description': '基于LangChain SQL Agent',
            'icon': '🔧'
        }
    }
    
    # 检查LangChain是否可用
    if importlib.util.find_spec("langchain_community") is None:
        schemes['langchain']['available'] = False
        schemes['langchain']['note'] = '需要安装: pip install langchain-community'
    else:
        schemes['langchain']['available'] = True
    
    return schemes


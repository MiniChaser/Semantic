import requests
import json
from typing import List, Dict, Any


class SemanticScholarDatasetAPI:
    """Semantic Scholar数据集API处理类"""
    
    BASE_URL = "https://api.semanticscholar.org/datasets/v1"
    
    def __init__(self):
        self.session = requests.Session()
    
    def get_releases(self) -> List[str]:
        """获取所有发布版本"""
        response = self.session.get(f"{self.BASE_URL}/release")
        response.raise_for_status()
        releases = response.json()
        return releases
    
    def get_latest_release(self) -> Dict[str, Any]:
        """获取最新发布版本信息"""
        response = self.session.get(f"{self.BASE_URL}/release/latest")
        response.raise_for_status()
        return response.json()
    
    def get_dataset_info(self, dataset_name: str) -> Dict[str, Any]:
        """获取特定数据集信息（需要API密钥）"""
        response = self.session.get(f"{self.BASE_URL}/release/latest/dataset/{dataset_name}")
        if response.status_code == 401:
            raise PermissionError("需要API密钥才能访问数据集文件信息")
        response.raise_for_status()
        return response.json()
    
    def print_recent_releases(self):
        """打印最近3个发布版本"""
        releases = self.get_releases()
        print("最近3个发布版本:")
        print(releases[-3:])
        return releases[-3:]
    
    def print_latest_release_info(self):
        """打印最新发布版本信息"""
        latest_release = self.get_latest_release()
        print(f"最新发布版本ID: {latest_release['release_id']}")
        return latest_release
    
    def print_dataset_details(self, dataset_name: str = "abstracts"):
        """打印数据集详细信息（需要API密钥）"""
        try:
            dataset_info = self.get_dataset_info(dataset_name)
            print(f"{dataset_name}数据集详细信息:")
            print(json.dumps(dataset_info, indent=2))
            return dataset_info
        except PermissionError as e:
            print(f"警告: {e}")
            print("数据集文件信息需要API密钥认证才能访问")
            return None


def main():
    """主函数 - 执行所有API调用示例"""
    api = SemanticScholarDatasetAPI()
    
    print("=" * 50)
    print("Semantic Scholar数据集API示例")
    print("=" * 50)
    
    # 示例1: 获取最近3个发布版本
    print("\n1. 获取最近3个发布版本:")
    api.print_recent_releases()
    
    # 示例2: 获取最新发布版本
    print("\n2. 获取最新发布版本信息:")
    latest_release = api.print_latest_release_info()
    
    # 示例3: 打印第一个数据集的详细信息
    if latest_release.get('datasets'):
        print("\n3. 第一个数据集的详细信息:")
        first_dataset = latest_release['datasets'][0]
        print(json.dumps(first_dataset, indent=2))
    
    # 示例4: 获取abstracts数据集的文件信息（需要API密钥）
    print("\n4. abstracts数据集文件信息:")
    dataset_files = api.print_dataset_details("abstracts")
    if dataset_files is None:
        print("跳过文件信息获取（需要API密钥）")


if __name__ == "__main__":
    main()
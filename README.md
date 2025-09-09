# DBLP语义数据处理管道 v2.0

这是一个现代化的DBLP数据处理管道，支持定时调度、增量更新和模块化架构。

## 功能特性

- **🚀 完整管道**: 下载、解压、解析、入库一站式处理
- **🔄 增量处理**: 支持增量更新，避免重复处理已存在数据
- **⏰ 定时调度**: 基于APScheduler的可配置定时任务
- **🏗️ 模块化架构**: 独立的服务组件，便于扩展和维护
- **📊 PostgreSQL支持**: 专为PostgreSQL优化的数据存储
- **⚙️ 环境配置**: 通过.env文件管理所有配置
- **📦 uv项目管理**: 使用现代Python包管理工具
- **📈 进度追踪**: 详细的进度条和日志记录
- **🛡️ 错误处理**: 完善的错误处理和重试机制
- **💾 批量处理**: 内存友好的批处理机制

## 项目架构

```
semantic/
├── src/semantic/           # 主要源代码
│   ├── database/          # 数据库相关模块
│   │   ├── connection.py  # 数据库连接管理
│   │   └── models.py      # 数据模型和仓库
│   ├── services/          # 业务服务层
│   │   ├── dblp_service.py      # DBLP数据处理服务
│   │   └── pipeline_service.py  # 数据管道服务
│   ├── scheduler/         # 任务调度
│   │   └── scheduler.py   # APScheduler调度器
│   └── utils/            # 工具模块
│       └── config.py     # 配置管理
├── scripts/              # 运行脚本
│   ├── run_scheduler.py   # 调度器启动脚本
│   └── run_pipeline_once.py # 单次运行脚本
├── config/              # 配置文件
├── logs/                # 日志文件
├── data/                # 数据文件
└── external/            # 外部下载文件
```

## 快速开始

### 1. 环境准备

```bash
# 安装uv (如果还没安装)
curl -LsSf https://astral.sh/uv/install.sh | sh

# 克隆/下载项目到本地
cd semantic

# 使用uv安装依赖
uv sync
```

### 2. 配置数据库

复制环境变量模板：
```bash
cp .env.example .env
```

编辑 `.env` 文件，填入你的PostgreSQL连接信息：
```bash
# PostgreSQL Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=dblp_semantic
DB_USER=postgres
DB_PASSWORD=your_password

# Processing Configuration
TARGET_VENUES=acl,naacl,emnlp,findings
ENABLE_VENUE_FILTER=true
BATCH_SIZE=10000
LOG_LEVEL=INFO

# Scheduling Configuration  
SCHEDULE_CRON=0 2 * * 1
ENABLE_INCREMENTAL=true
```

### 3. 运行管道

#### 单次运行
```bash
# 使用脚本运行一次完整管道
./scripts/run_pipeline_once.py

# 或者使用uv运行
uv run python scripts/run_pipeline_once.py
```

#### 启动定时调度器
```bash
# 启动定时调度器（按配置的cron表达式运行）
./scripts/run_scheduler.py

# 手动执行一次（立即执行）
./scripts/run_scheduler.py --manual

# 列出所有任务
./scripts/run_scheduler.py --list-jobs
```

#### 直接使用Python模块
```bash
# 进入虚拟环境
uv shell

# 使用Python模块
python -m semantic.scheduler.scheduler --manual
```

## 配置选项

### 数据库配置
- `DB_HOST`: PostgreSQL主机地址
- `DB_PORT`: PostgreSQL端口
- `DB_NAME`: 数据库名称
- `DB_USER`: 数据库用户名
- `DB_PASSWORD`: 数据库密码

### 处理配置
- `TARGET_VENUES`: 目标会议列表（逗号分隔）
- `ENABLE_VENUE_FILTER`: 是否启用会议筛选（true/false）
- `BATCH_SIZE`: 批处理大小（默认10000）
- `LOG_LEVEL`: 日志级别（INFO/DEBUG/WARNING/ERROR）

### 调度配置
- `SCHEDULE_CRON`: Cron表达式（默认: 0 2 * * 1，每周一凌晨2点）
- `MAX_RETRIES`: 最大重试次数（默认3）
- `RETRY_DELAY`: 重试延迟秒数（默认300）

### 增量处理配置
- `ENABLE_INCREMENTAL`: 是否启用增量处理（true/false）
- `INCREMENTAL_CHECK_DAYS`: 增量检查天数（默认7）

## 数据库表结构

### 主要数据表
```sql
CREATE TABLE dblp_papers (
    id SERIAL PRIMARY KEY,
    key VARCHAR(255) UNIQUE NOT NULL,
    title TEXT NOT NULL,
    authors JSONB NOT NULL,
    author_count INTEGER,
    venue VARCHAR(50),
    year VARCHAR(4),
    pages VARCHAR(50),
    ee TEXT,
    booktitle TEXT,
    doi VARCHAR(100),
    created_at TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 处理元数据表
```sql
CREATE TABLE dblp_processing_meta (
    id SERIAL PRIMARY KEY,
    process_type VARCHAR(50) NOT NULL,
    last_run_time TIMESTAMP NOT NULL,
    status VARCHAR(20) NOT NULL,
    records_processed INTEGER DEFAULT 0,
    records_inserted INTEGER DEFAULT 0,
    records_updated INTEGER DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 调度器作业表
```sql
CREATE TABLE scheduler_jobs (
    -- APScheduler自动创建的表结构
);
```

## 工作流程

### 增量处理流程
1. **检查上次运行时间**: 从`dblp_processing_meta`表获取上次成功运行时间
2. **决定处理模式**: 根据配置和时间间隔决定是全量还是增量处理
3. **数据准备**: 下载和解压DBLP数据文件
4. **增量解析**: 只处理不存在于数据库的新论文
5. **批量更新**: 使用UPSERT操作批量插入或更新数据
6. **记录元数据**: 记录本次处理的统计信息

### 调度器工作流程
1. **初始化**: 连接数据库，设置作业存储
2. **作业注册**: 根据Cron表达式注册定时任务
3. **任务执行**: 在指定时间执行数据管道
4. **错误处理**: 失败时自动重试，记录错误日志
5. **状态监控**: 监听作业执行状态，生成执行报告

## 使用示例

### 编程接口使用
```python
from semantic.services.pipeline_service import DataPipelineService
from semantic.utils.config import AppConfig
from semantic.database.connection import get_db_manager

# 创建配置
config = AppConfig.from_env()

# 创建管道服务
pipeline = DataPipelineService(config)

# 运行完整管道
success = pipeline.run_pipeline()

# 导出数据
if success:
    pipeline.export_to_csv("output/papers.csv")
```

### 独立模块使用
```python
from semantic.services.dblp_service import DBLPService
from semantic.database.models import PaperRepository
from semantic.database.connection import get_db_manager

# 使用DBLP服务
dblp_service = DBLPService(config)
papers = dblp_service.parse_papers()

# 使用数据库仓库
db_manager = get_db_manager()
paper_repo = PaperRepository(db_manager)
paper_repo.batch_insert_papers(papers)
```

## 开发指南

### 安装开发依赖
```bash
uv sync --dev
```

### 代码格式化
```bash
uv run black src/
uv run isort src/
```

### 代码检查
```bash
uv run flake8 src/
```

### 运行测试
```bash
uv run pytest tests/
```

## 扩展和集成

### 添加新的数据源
1. 在`src/semantic/services/`中创建新的服务类
2. 实现统一的数据接口（继承基础服务类）
3. 在管道服务中集成新的数据源

### 添加新的数据处理步骤
1. 在管道服务中添加新的步骤方法
2. 更新`run_pipeline()`方法的执行流程
3. 添加相应的配置选项和错误处理

### 集成其他调度系统
1. 实现自定义的调度器类
2. 保持与现有管道服务的接口兼容
3. 提供相同的监控和日志功能

## 监控和日志

### 日志文件
- 管道执行日志: `logs/dblp_service_YYYYMMDD_HHMMSS.log`
- 调度器日志: `logs/scheduler_YYYYMMDD.log`
- 数据库操作日志: 集成在管道日志中

### 监控指标
- 处理论文数量
- 新增/更新论文统计
- 执行时间
- 错误率
- 重试次数

### 数据库监控
```sql
-- 查看处理历史
SELECT * FROM dblp_processing_meta ORDER BY created_at DESC LIMIT 10;

-- 查看数据统计
SELECT venue, COUNT(*) as count 
FROM dblp_papers 
GROUP BY venue 
ORDER BY count DESC;
```

## 故障排除

### 常见问题

**连接失败**
- 检查PostgreSQL服务是否运行
- 验证.env文件中的连接信息
- 确认数据库存在且用户有权限

**下载失败**
- 检查网络连接
- 验证DBLP URL是否可访问
- 检查磁盘空间

**调度器启动失败**
- 检查Cron表达式格式
- 验证数据库连接
- 查看调度器日志文件

**增量处理异常**
- 检查元数据表是否存在
- 验证增量配置参数
- 清理临时文件重新运行

### 性能优化建议

1. **数据库优化**
   - 定期执行VACUUM ANALYZE
   - 监控索引使用情况
   - 调整批处理大小

2. **内存优化**
   - 根据服务器内存调整批处理大小
   - 监控内存使用情况
   - 及时清理临时文件

3. **网络优化**
   - 使用稳定的网络连接
   - 考虑使用代理服务器
   - 设置合适的超时时间

## 许可证

此项目遵循MIT许可证。

## 贡献指南

欢迎提交Pull Request和Issue！请确保：

1. 代码符合项目的格式规范
2. 添加适当的测试用例
3. 更新相关文档
4. 遵循现有的架构模式

## 更新日志

### v2.0.0
- 重构为模块化架构
- 添加增量处理功能
- 集成APScheduler定时调度
- 增强错误处理和重试机制
- 完善日志和监控系统

### v1.0.0
- 基础DBLP数据处理功能
- PostgreSQL数据存储
- 基本的批处理机制
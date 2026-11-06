# AGENTS.md

给在本仓库里干活的 agent 看的约定。人类协作者也适用。

## 这个仓库是什么

AQI（空气质量指数）预测的教学/研究项目：把 NOAA GSOD 气象数据和 OpenAQ 污染物数据拼成训练表，用 AutoGluon 训练回归模型预测 AQI。

数据来源两个：NOAA GSOD（气象）、OpenAQ（污染物）。两者都按站点、按 2024 年全年取数。

## 目录地图

| 路径 | 内容 | 谁生成 |
| --- | --- | --- |
| `aqi_breakpoints.py` | EPA 各污染物 AQI 断点表 | 手写常量 |
| `retirval_gsod_data.py` → `noaa_data/` | GSOD 原始数据（按站点一文件） | 脚本拉取 |
| `eda_gsod_data.py` → `eda_output/` | 原始数据的 EDA 图表 | 脚本生成 |
| `post_eda_gsod_data.py` → `post_eda_noaa_data/` | EDA 后的聚合/清洗结果 | 脚本生成 |
| `retrival_openaq_data.py` → `openaq_data/` | 单污染物原始数据（`day,average,coverage,unit,parameter`） | 脚本拉取 |
| `integrate_pollutants.py` → `openaq_integrated_data/` | 按站点整合的宽表（`datetime,pm25,pm10,o3,co,so2,no2`） | 脚本生成 |
| `calculate_aqi.py` → `aqi_data/` | 浓度 + 分项 AQI + 综合 AQI | 脚本生成 |
| `merge_gsod_aqi_data.py`、`merge_all_gsod_aqi_data.py` → `all_gsod_aqi_merged_data/` | 训练表（`SITE,STATION,DATE,...,AQI`） | 脚本生成 |
| `aqi_predictor.py` → `autogluon_aqi_predictor/` | 训练产物与推理 | 脚本生成 |
| `clean_aqi_data.py`、`tests/` | 数据清洗脚本与其单测 | 人/agent |

执行顺序就是 README 里的 1→10；每一步的输出目录是下一步的输入。

## 环境与运行

- Python 3.10+。训练部分需要 `pandas`、`numpy`、`autogluon`。
- 清洗脚本 `clean_aqi_data.py` **只依赖标准库**——清洗是最前置的一步，不该因为没装 autogluon 就跑不起来。
- 仓库里没有 `requirements.txt`，装依赖请按需 `pip install pandas numpy autogluon`，不要擅自升级大版本。
- 跑测试（仓库根目录）：

  ```bash
  python -m unittest discover -s tests -v
  ```

## 数据清洗脚本

```bash
python clean_aqi_data.py --input aqi_data/bakersfield_aqi_2024_2024.csv --output cleaned/bakersfield.csv
python clean_aqi_data.py --input openaq_data/x.csv --output y.csv --min-coverage 75 --report y.report.json
```

- 按表头自动识别四种形态（`openaq` / `integrated` / `aqi` / `merged`），也可用 `--kind` 强制指定。
- 清洗规则写在脚本顶部的常量里：负浓度、超物理上限、缺失值、重复日期、越界 AQI、越界气象值分别计数并写进报告。
- **原文件只读**：输出永远写到新路径，不覆盖 `aqi_data/`、`openaq_data/` 等目录里的原始数据。

## 代码约定

- 扁平的顶层脚本，一个文件一步，能被 `python xxx.py` 直接跑起来。
- 注释、日志、报错信息用中文，和现有脚本保持一致。
- 新增处理步骤要写成独立脚本并更新 README 的步骤列表，不要把逻辑塞进已有脚本。
- 输出目录、文件命名沿用现有模式：`<site>_<stage>_<yearStart>_<yearEnd>.csv`。
- 不新增第三方依赖，除非确实无法用标准库或已有依赖完成。

## 完成标准（Definition of Done）

一条 issue 算完成，必须同时满足：

1. **改了什么**：列出改动文件，每个文件一句话说明为什么改。
2. **怎么验证的**：贴出跑过的命令与结果（测试输出、脚本运行输出）。跑不了要写清为什么跑不了。
3. **回写到哪**：结论写回对应 issue/PR，并打「主理鱼:已处理」标签。

没有验证结果不算完成。测试失败要么修到通过，要么在回写里说明失败原因与影响范围。

## 修 / 回 / 待定

被分配一条 issue 时，先判断属于哪一类，再动手：

- **修**：问题能在本仓库内复现或明确成立，范围落在单个脚本/文档改动内 —— 直接改，附验证。
- **回**：缺关键信息（复现步骤、期望值、数据样本），或判断「这不是本仓库的问题」—— 在 issue 里提出具体问题或给出结论，不要猜着改。
- **待定**：依赖外部条件（数据源不可达、缺少凭据、上游库 bug、需要重跑整条流水线）—— 记录阻塞点和解除条件，保持 open。

拿不准时按「回」处理：问清楚比改错强。

## 护栏

- 不要提交新的模型权重、数据集快照或二进制大文件；仓库里已有的历史产物保持不动。
- 不要提交密钥、token、`.env`；`gh` 凭据只放本机。
- 不要改动 `aqi_breakpoints.py` 里的 EPA 断点值——那是标准，不是可调参数。
- 不扩大 issue 范围：顺手发现的其它问题，另开 issue。

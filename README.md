#心脏病风险监测分析平台

> 大数据全流程项目 · 数仓四层架构 · ODS → DWD → DWS → ADS

---

## 一、项目简介

本平台面向普通用户、医院、社区卫生服务中心，利用大数据技术对海量健康数据进行集中处理，自动分析每位用户的心脏病风险等级，实现**早发现、早提醒、早干预**。

---

## 二、数据源说明（四份真实数据集）

| 文件名 | 记录数 | 字段数 | 说明 |
|--------|--------|--------|------|
| CAD.csv | 303 | 55 | 冠状动脉造影数据集，含血压、血脂、射血分数等 |
| heart_2020_cleaned.csv | 319,795 | 18 | 2020美国心脏健康调查，含BMI、吸烟、糖尿病等 |
| heart_statlog_cleveland_hungary_final.csv | 1,190 | 12 | Cleveland+Hungary合并数据集，含胆固醇、ST段等 |
| Medicaldataset.csv | 1,319 | 9 | 医学急诊心脏标志物，含CK-MB、肌钙蛋白 |
| **合并后（ods_merged_all.csv）** | **322,607** | **16** | 四表标准化合并，心脏病阳性29,028人 |

---

## 三、技术架构

```
原始CSV数据
    ↓
[数据整合] merge_data.py  ←  Python pandas 四表标准化合并
    ↓
[ODS层]  HDFS + Hive     ←  原始数据存储，不做任何修改（5张表）
    ↓
[DWD层]  Hive HQL        ←  数据清洗：去重/脱敏/填充缺失值/过滤异常
    ↓
[DWS层]  Spark SQL       ←  聚合宽表：均值/最大值/衍生字段/危险因素标记
    ↓
[ADS层]  Spark SQL       ←  风险评分（8维度规则引擎，满分100）+ 风险等级
    ↓
[结果存储] MySQL         ←  4张业务表，供前端查询
    ↓
[可视化]  ECharts 5.x   ←  7个图表暗色大屏
```

---

## 四、文件目录结构

```
heart_platform/
├── README.md                        ← 本说明文件
│
├── data/                            ← 数据处理层
│   ├── merge_data.py                ← 四表合并标准化脚本（已可执行）
│   ├── ods_raw_cad.csv              ← ODS原始：CAD数据
│   ├── ods_raw_heart2020.csv        ← ODS原始：Heart2020数据
│   ├── ods_raw_statlog.csv          ← ODS原始：Statlog数据
│   ├── ods_raw_medical.csv          ← ODS原始：Medical数据
│   └── ods_merged_all.csv           ← ODS合并：322,607条统一格式数据
│
├── hive/                            ← Hive SQL脚本
│   ├── create_ods.sql               ← ODS层建表（5张外部表）
│   └── dwd_etl.sql                  ← DWD层清洗ETL（去重/脱敏/异常过滤）
│
├── spark/                           ← Spark PySpark脚本
│   ├── dws_spark.py                 ← DWS层聚合宽表（衍生字段/统计值）
│   ├── ads_risk.py                  ← ADS层风险评分模型（规则引擎）
│   └── export_to_mysql.py           ← ADS结果导出到MySQL（4张统计表）
│
├── mysql/                           ← MySQL建表脚本
│   └── create_tables.sql            ← 4张业务表（详情/汇总/年龄段/因素统计）
│
└── frontend/                        ← 前端可视化
    ├── dashboard.html               ← ECharts大屏（7个图表）★ 主要展示文件
    └── chart_data.json              ← 真实统计数据（322,607条计算结果）
```

---

## 五、完整执行步骤

### Step 0：数据准备（本地，已完成）
```bash
# 将四份原始CSV放入data/目录，然后执行合并
python data/merge_data.py
# 输出：data/ods_merged_all.csv（322,607条，27MB）
```

### Step 1：上传HDFS + ODS建表
```bash
# 创建HDFS目录
hdfs dfs -mkdir -p /warehouse/heart_platform/ods/merged_all
hdfs dfs -mkdir -p /warehouse/heart_platform/dwd/health_clean
hdfs dfs -mkdir -p /warehouse/heart_platform/dws/user_health_wide
hdfs dfs -mkdir -p /warehouse/heart_platform/ads/risk_result

# 上传合并数据
hdfs dfs -put data/ods_merged_all.csv /warehouse/heart_platform/ods/merged_all/

# Hive建表
hive -f hive/create_ods.sql
```

### Step 2：DWD层数据清洗
```bash
hive -f hive/dwd_etl.sql
```
**清洗内容：**
- ① 手机号脱敏（138****8888格式）
- ② 缺失值填充（按数据来源分组取均值）
- ③ 去重（同一用户保留最新记录）
- ④ 异常值过滤（血压60~250mmHg、心率20~250bpm等）
- ⑤ 日期格式统一（yyyy-MM-dd）

### Step 3：DWS层聚合宽表
```bash
spark-submit --master yarn --executor-memory 4g --num-executors 3 spark/dws_spark.py
```
**聚合内容：**
- 各指标均值/最大值/最小值
- 年龄段（青年/中年/中老年/老年）
- BMI等级（偏瘦/正常/超重/肥胖）
- 血压等级（低血压/正常/高血压1/2/3级）
- 危险因素计数

### Step 4：ADS层风险评分
```bash
spark-submit --master yarn --executor-memory 4g --num-executors 3 spark/ads_risk.py
```
**风险评分维度（总分100）：**

| 维度 | 最高分 | 规则 |
|------|--------|------|
| 年龄 | 25分 | ≥70岁25分，≥65岁20分，≥55岁15分 |
| 血压 | 20分 | ≥180mmHg满分，≥140mmHg12分 |
| 血糖 | 10分 | ≥200mg/dL满分，≥126mg/dL7分 |
| 胆固醇 | 10分 | ≥280mg/dL满分，≥240mg/dL7分 |
| BMI | 5分 | ≥35满分，≥28共3分 |
| 吸烟 | 15分 | 吸烟者15分 |
| 糖尿病史 | 10分 | 有糖尿病史10分 |
| 家族病史 | 5分 | 有家族史5分 |

**风险等级：**
- 🟢 低风险：0~34分
- 🟡 中风险：35~59分
- 🔴 高风险：60~100分

### Step 5：结果写入MySQL
```bash
# 先建表
mysql -u root -p heart_platform < mysql/create_tables.sql

# 再导出（需带mysql-connector.jar）
spark-submit --master yarn \
  --jars /opt/jars/mysql-connector-java-8.0.33.jar \
  spark/export_to_mysql.py
```
> ⚠️ 注意修改 `export_to_mysql.py` 中的 `your_password` 为你的MySQL密码

## Step 6：前端可视化
```bash
# 用浏览器直接打开（双击即可）
frontend/dashboard.html
```
**包含7个图表：**
1. 风险等级分布（环形饼图）
2. 各年龄段风险分布（堆叠柱状图）
3. 数据来源分布（饼图）
4. 危险因素人群占比（横向条形图）
5. 高/低风险组对比（雷达图）
6. 综合风险评分（仪表盘）
7. 心脏病阳性率按年龄段（柱状图）

---

## 六、核心数据统计结果

| 指标 | 数值 |
|------|------|
| 总用户数 | 322,607 |
| 低风险用户 | 251,424（77.9%） |
| 中风险用户 | 71,174（22.1%） |
| 高风险用户 | 9（0.0%） |
| 心脏病阳性 | 29,028 |
| 心脏病阴性 | 293,579 |
| 平均年龄 | 54.7岁 |
| 平均BMI | 28.3 |
| 平均收缩压 | 129.5 mmHg |
| 平均心率 | 104.0 bpm |
| 平均风险评分 | 22.8分 |

---

## 七、技术栈版本

| 组件 | 版本 | 说明 |
|------|---------|------|
| Hadoop | 3.3.x | HDFS分布式存储 |
| Hive | 3.1.x | 离线SQL计算 |
| Spark | 3.3.x | 内存计算引擎 |
| Python | 3.8+ | 数据处理脚本 |
| MySQL | 8.0.x | 结果存储 |
| ECharts | 5.4.x | 前端可视化 |

---

## 八、数据字段说明（统一标准化字段）

| 字段名 | 类型 | 说明 | 来源 |
|--------|------|------|------|
| user_id | STRING | 用户唯一ID | 自动生成 |
| age | INT | 年龄（岁） | 所有数据源 |
| gender | INT | 性别（1男0女） | 所有数据源 |
| blood_pressure | DOUBLE | 收缩压（mmHg） | CAD/Statlog/Medical |
| heart_rate | DOUBLE | 心率（bpm） | Statlog/Medical |
| blood_sugar | DOUBLE | 血糖（mg/dL） | CAD/Statlog/Medical |
| cholesterol | DOUBLE | 胆固醇（mg/dL） | Statlog |
| bmi | DOUBLE | 体质量指数 | CAD/Heart2020 |
| smoking_flag | INT | 吸烟（1是0否） | CAD/Heart2020 |
| diabetes_flag | INT | 糖尿病（1是0否） | CAD/Heart2020 |
| family_history | INT | 家族史（1有0无） | CAD |
| heart_disease_flag | INT | 心脏病诊断（1有0无） | 所有数据源 |
| data_source | STRING | 数据来源标记 | 自动标记 |

---

*项目组：心脏病风险监测分析平台*

# ES ICT/SMC 因果事件研究 · 2026-08-31

本报告只研究客观 session level 的 Sweep → MSS proxy → Displacement → FVG retrace。
所有信号在实际可知时刻发布，下一分钟入场；不读取生产候选，也不赋予交易权限。

## 数据覆盖

- 完整 RTH sessions：33
- 可用 prior/overnight levels 的 sessions：32
- 原始逐 level sweep：95
- 5 分钟同向去重后：87

## 消融结果

| 阶段 | 样本/会话 | 15m均值 | 15m对照增量 | 30m均值 | 30m对照增量 | 30m bootstrap 95% |
|---|---:|---:|---:|---:|---:|---:|
| sweep | 87/32 | 0.73 | 1.49 | 2.24 | 4.08 | [-2.56, 10.06] |
| sweep_mss | 41/27 | 1.88 | 1.82 | 3.63 | 3.72 | [0.79, 12.80] |
| sweep_mss_displacement | 28/20 | 2.04 | 2.22 | 3.72 | 5.94 | [1.38, 17.10] |
| sweep_mss_displacement_fvg | 17/12 | 2.76 | 5.50 | 1.59 | 6.99 | [-2.29, 24.22] |
| sweep_mss_displacement_fvg_htf | 10/8 | 4.95 | 5.48 | 0.88 | 2.76 | [-4.70, 15.29] |

## 时段外推

| 阶段 | 分段 | 样本/会话 | 15m均值 | 15m对照增量 | 30m均值 | 30m对照增量 |
|---|---|---:|---:|---:|---:|---:|
| sweep | development | 60/21 | 0.09 | 0.71 | 1.20 | 2.81 |
| sweep | validation | 13/6 | 0.98 | 3.06 | 2.87 | 6.23 |
| sweep | tail | 14/5 | 3.23 | 3.30 | 6.09 | 7.32 |
| sweep_mss | development | 28/18 | 2.40 | 2.10 | 4.12 | 3.93 |
| sweep_mss | validation | 6/5 | 1.21 | 2.72 | 2.58 | 6.28 |
| sweep_mss | tail | 7/4 | 0.36 | -0.06 | 2.57 | 0.75 |
| sweep_mss_displacement | development | 18/13 | 2.61 | 3.01 | 4.01 | 7.29 |
| sweep_mss_displacement | validation | 4/3 | 1.94 | 3.48 | 3.94 | 9.65 |
| sweep_mss_displacement | tail | 6/4 | 0.42 | -0.88 | 2.71 | -0.35 |
| sweep_mss_displacement_fvg | development | 12/8 | 2.62 | 5.63 | 3.27 | 8.75 |
| sweep_mss_displacement_fvg | validation | 2/2 | 0.25 | 3.92 | -3.50 | 6.17 |
| sweep_mss_displacement_fvg | tail | 3/2 | 5.00 | 6.08 | -1.75 | 1.08 |
| sweep_mss_displacement_fvg_htf | development | 5/4 | 6.80 | 5.81 | 4.20 | 2.31 |
| sweep_mss_displacement_fvg_htf | validation | 2/2 | 0.25 | 3.92 | -3.50 | 6.17 |
| sweep_mss_displacement_fvg_htf | tail | 3/2 | 5.00 | 6.08 | -1.75 | 1.08 |

## SPXW Directional Spread exact-BBO 消融

预先固定比较最接近 40Δ/50Δ/60Δ 的 long leg，均为 15 点 Debit Vertical；信号后 5 秒按保守 ask 入场，固定 15/30 分钟按保守 bid 离场，含双边手续费，不事后选择最佳 Delta。

| Long Δ | ICT 阶段 | 信号→入场/会话 | C/P | 15m均值$ | 30m均值$ | 30m胜率 | 30m CVaR10$ | 30m bootstrap 95% |
|---:|---|---:|---:|---:|---:|---:|---:|---:|
| 40Δ | sweep | 87→57/28 | 31/26 | -30.99 | -13.88 | 45.6% | -356.11 | [-79.21, 60.05] |
| 40Δ | sweep_mss | 41→24/19 | 12/12 | -9.65 | 26.80 | 58.3% | -205.28 | [-35.28, 110.38] |
| 40Δ | sweep_mss_displacement | 28→15/13 | 8/7 | 13.72 | 57.72 | 66.7% | -172.78 | [-15.28, 150.49] |
| 40Δ | sweep_mss_displacement_fvg | 17→11/9 | 5/6 | -18.01 | 2.90 | 36.4% | -252.78 | [-163.06, 239.19] |
| 40Δ | sweep_mss_displacement_fvg_htf | 10→4/4 | 0/4 | -1.53 | 23.47 | 50.0% | -230.28 | [-217.78, 334.72] |
| 50Δ | sweep | 87→33/21 | 11/22 | 24.56 | 68.20 | 57.6% | -250.28 | [-29.92, 106.15] |
| 50Δ | sweep_mss | 41→18/14 | 6/12 | -34.45 | 5.00 | 55.6% | -227.78 | [-95.99, 87.05] |
| 50Δ | sweep_mss_displacement | 28→13/10 | 5/8 | -30.66 | 18.18 | 69.2% | -227.78 | [-74.54, 132.47] |
| 50Δ | sweep_mss_displacement_fvg | 17→6/6 | 0/6 | -40.28 | -79.45 | 16.7% | -270.28 | [-242.78, 158.05] |
| 50Δ | sweep_mss_displacement_fvg_htf | 10→4/4 | 0/4 | -32.78 | -66.53 | 25.0% | -270.28 | [-262.78, 299.72] |
| 60Δ | sweep | 87→10/9 | 0/10 | -65.78 | -87.28 | 40.0% | -465.28 | [-229.72, 19.16] |
| 60Δ | sweep_mss | 41→10/10 | 0/10 | 11.22 | 5.72 | 60.0% | -260.28 | [-107.78, 118.72] |
| 60Δ | sweep_mss_displacement | 28→8/8 | 0/8 | 32.22 | 52.84 | 75.0% | -260.28 | [-71.53, 164.11] |
| 60Δ | sweep_mss_displacement_fvg | 17→2/2 | 0/2 | -45.28 | -70.28 | 50.0% | -195.28 | [-195.28, 54.72] |
| 60Δ | sweep_mss_displacement_fvg_htf | 10→1/1 | 0/1 | 24.72 | 54.72 | 100.0% | 54.72 | [—, —] |

| Long Δ | ICT 阶段 | 分段 | 定价笔数/会话 | 30m均值$ |
|---:|---|---|---:|---:|
| 40Δ | sweep | development | 41/19 | -46.13 |
| 40Δ | sweep | validation | 6/5 | 0.55 |
| 40Δ | sweep | tail | 10/4 | 109.72 |
| 40Δ | sweep_mss_displacement | development | 8/7 | 67.85 |
| 40Δ | sweep_mss_displacement | validation | 2/2 | 112.22 |
| 40Δ | sweep_mss_displacement | tail | 5/4 | 19.72 |
| 50Δ | sweep | development | 17/11 | 37.96 |
| 50Δ | sweep | validation | 6/5 | 5.55 |
| 50Δ | sweep | tail | 10/5 | 157.22 |
| 50Δ | sweep_mss_displacement | development | 5/4 | 12.72 |
| 50Δ | sweep_mss_displacement | validation | 2/2 | -72.78 |
| 50Δ | sweep_mss_displacement | tail | 6/4 | 53.05 |
| 60Δ | sweep | development | 7/6 | -125.99 |
| 60Δ | sweep | validation | 2/2 | -62.78 |
| 60Δ | sweep | tail | 1/1 | 134.72 |
| 60Δ | sweep_mss_displacement | development | 4/4 | 59.72 |
| 60Δ | sweep_mss_displacement | validation | 2/2 | -62.78 |
| 60Δ | sweep_mss_displacement | tail | 2/2 | 154.72 |

### 同一事件相对 Sweep 的配对差异

正值才表示后续确认改变了入场/合约后的 PnL；零值表示表面改善主要来自删掉其他 Sweep，而非更好的入场时点。

| Long Δ | 后续阶段 | 30m配对/会话 | 30m增量均值$ | 30m增量中位$ |
|---:|---|---:|---:|---:|
| 40Δ | sweep_mss | 22/18 | -22.95 | 0.00 |
| 40Δ | sweep_mss_displacement | 14/13 | -36.07 | 0.00 |
| 40Δ | sweep_mss_displacement_fvg | 6/5 | -33.33 | -42.50 |
| 40Δ | sweep_mss_displacement_fvg_htf | 1/1 | -60.00 | -60.00 |
| 50Δ | sweep_mss | 17/13 | -47.94 | 0.00 |
| 50Δ | sweep_mss_displacement | 12/9 | -66.25 | 0.00 |
| 50Δ | sweep_mss_displacement_fvg | 3/3 | -133.33 | -30.00 |
| 50Δ | sweep_mss_displacement_fvg_htf | 2/2 | -185.00 | -185.00 |
| 60Δ | sweep_mss | 8/8 | 0.00 | 0.00 |
| 60Δ | sweep_mss_displacement | 6/6 | 0.00 | 0.00 |
| 60Δ | sweep_mss_displacement_fvg | 2/2 | -55.00 | -55.00 |
| 60Δ | sweep_mss_displacement_fvg_htf | 1/1 | -80.00 | -80.00 |

- 40Δ Sweep 未入场：fixed_15_point_short_leg_missing=28；no_30_to_80_delta_long_leg=2
- 50Δ Sweep 未入场：fixed_15_point_short_leg_missing=36；debit_fraction_above_45pct=16；no_30_to_80_delta_long_leg=2
- 60Δ Sweep 未入场：debit_fraction_above_45pct=45；fixed_15_point_short_leg_missing=30；no_30_to_80_delta_long_leg=2

## Sweep 来源拆分

| 分组 | 样本/会话 | 15m均值 | 30m均值 | 30m对照增量 | Dev/Val/Tail 30m |
|---|---:|---:|---:|---:|---:|
| direction:bearish | 45/26 | 0.65 | 1.97 | 1.29 | 30:0.11/7:3.50/8:7.59 |
| direction:bullish | 42/25 | 0.82 | 2.52 | 7.21 | 30:2.29/6:2.12/6:4.08 |
| level:ONH | 13/13 | 0.90 | 1.15 | -1.65 | 9:-1.47/2:6.50/2:7.62 |
| level:ONL | 14/14 | -2.21 | 3.32 | 11.96 | 10:5.35/2:-6.12/2:2.62 |
| level:OR15H | 19/19 | -1.63 | -3.84 | -2.07 | 11:-9.14/5:2.30/3:5.33 |
| level:OR15L | 21/21 | 2.76 | 1.98 | 6.03 | 15:-0.07/3:8.33/3:5.83 |
| level:PDH | 13/13 | 3.73 | 11.27 | 9.15 | 10:11.70/0:—/3:9.83 |
| level:PDL | 7/7 | 1.04 | 2.57 | 1.77 | 5:3.25/1:0.00/1:1.75 |
| time:09:30-10:30 | 54/31 | -0.31 | 1.88 | 4.11 | 38:1.11/7:1.64/9:5.28 |
| time:10:30-12:00 | 20/16 | 1.95 | 3.81 | 5.83 | 13:3.17/5:3.95/2:7.62 |
| time:12:00-14:00 | 8/8 | 3.91 | 6.19 | 2.91 | 5:5.70/1:6.00/2:7.50 |
| time:14:00-15:00 | 5/4 | 1.95 | -6.50 | -5.17 | 4:-10.00/0:—/1:7.50 |

## Sweep 参数敏感性（不择优）

| 最小穿越 | 回收分钟 | 样本/会话 | 15m均值 | 15m对照增量 | 30m均值 | 30m对照增量 |
|---:|---:|---:|---:|---:|---:|---:|
| 0.5 | 1 | 86/32 | 1.08 | 1.43 | 2.45 | 4.14 |
| 0.5 | 3 | 87/32 | 0.73 | 1.49 | 2.24 | 4.08 |
| 0.5 | 5 | 86/32 | 0.44 | 1.17 | 2.22 | 3.87 |
| 1.0 | 1 | 84/32 | 1.43 | 1.87 | 1.74 | 2.70 |
| 1.0 | 3 | 82/32 | 0.43 | 1.56 | 1.96 | 3.13 |
| 1.0 | 5 | 81/32 | 0.09 | 1.33 | 2.05 | 3.25 |
| 2.0 | 1 | 62/30 | 1.15 | 2.86 | 1.48 | 4.00 |
| 2.0 | 3 | 67/31 | -0.14 | 1.55 | 1.03 | 2.59 |
| 2.0 | 5 | 67/31 | 0.04 | 1.85 | 0.98 | 2.91 |

## 研究边界

- 主研究是 ES 方向事件；SPXW exact-BBO 预先固定比较 40Δ/50Δ/60Δ、15 点结构，各阶段样本仍小，且三档比较带来选择偏差，不能代表一般期权策略 edge。
- `MSS` 是严格因果的五根 K 结构突破代理，不声称等于某位交易员的主观画法。
- FVG 形成后必须等待下一根或更晚 K 线回踩；不允许同根 K 线事后假设理想成交。
- 多个参数和阶段属于探索性比较；任何 bootstrap 区间跨零的结果都不能称为 edge。
- 报告使用 quote-derived OHLC，不等同 CME trade/MBO；微观成交顺序仍有限制。

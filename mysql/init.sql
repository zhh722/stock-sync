-- 创建日线表（完整字段）
CREATE TABLE IF NOT EXISTS `stock_daily` (
  `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
  `code` VARCHAR(20) NOT NULL COMMENT '6位股票代码',
  `date` DATE NOT NULL,
  `open` DECIMAL(10,4),
  `high` DECIMAL(10,4),
  `low` DECIMAL(10,4),
  `close` DECIMAL(10,4),
  `preclose` DECIMAL(10,4),
  `volume` BIGINT COMMENT '成交量（股）',
  `amount` DECIMAL(18,2) COMMENT '成交额（元）',
  `adjustflag` TINYINT COMMENT '复权类型：3=前复权',
  `turn` DECIMAL(10,6) COMMENT '换手率（小数形式）',
  `tradestatus` TINYINT COMMENT '交易状态：1=正常',
  `pctChg` DECIMAL(10,4) COMMENT '涨跌幅（%）',
  `peTTM` DECIMAL(12,4),
  `pbMRQ` DECIMAL(12,4),
  `psTTM` DECIMAL(12,4),
  `pcfNcfTTM` DECIMAL(12,4),
  `isST` TINYINT,
  UNIQUE KEY `uk_code_date` (`code`, `date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 创建周线表（结构相同）
CREATE TABLE IF NOT EXISTS `stock_weekly` (
  `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
  `code` VARCHAR(20) NOT NULL COMMENT '6位股票代码',
  `date` DATE NOT NULL,
  `open` DECIMAL(10,4),
  `high` DECIMAL(10,4),
  `low` DECIMAL(10,4),
  `close` DECIMAL(10,4),
  `volume` BIGINT COMMENT '成交量（股）',
  `amount` DECIMAL(18,2) COMMENT '成交额（元）',
  `adjustflag` TINYINT COMMENT '复权类型：3=前复权',
  `turn` DECIMAL(10,6) COMMENT '换手率（小数形式）',
  `pctChg` DECIMAL(10,4) COMMENT '涨跌幅（%）',
  UNIQUE KEY `uk_code_date` (`code`, `date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 创建 1 分钟分时表
CREATE TABLE IF NOT EXISTS `stock_intraday_1m` (
  `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
  `code` VARCHAR(20) NOT NULL COMMENT '6位股票代码',
  `name` VARCHAR(64) DEFAULT NULL COMMENT '股票名称',
  `trade_date` DATE NOT NULL COMMENT '交易日期',
  `trade_time` TIME NOT NULL COMMENT '分钟时间',
  `trade_datetime` DATETIME NOT NULL COMMENT '分钟时间戳',
  `open` DECIMAL(10,4) DEFAULT NULL,
  `close` DECIMAL(10,4) DEFAULT NULL,
  `high` DECIMAL(10,4) DEFAULT NULL,
  `low` DECIMAL(10,4) DEFAULT NULL,
  `volume_hand` DECIMAL(20,4) DEFAULT NULL COMMENT '成交量（手）',
  `turnover_rate_pct` DECIMAL(12,6) DEFAULT NULL COMMENT '换手率（%）',
  `source` VARCHAR(32) NOT NULL DEFAULT 'tencent_mkline_m1',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY `uk_code_trade_datetime` (`code`, `trade_datetime`),
  KEY `idx_trade_date` (`trade_date`),
  KEY `idx_code_trade_date` (`code`, `trade_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

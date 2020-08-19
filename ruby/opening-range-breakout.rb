require 'csv'
require "bigdecimal"

require 'rubygems'
require 'bundler/setup'
require 'pry'
Bundler.require

file_name = "../mes-july2020.csv"

data = CSV.read(file_name, headers: true)

# system configs
STOP_LOSS_OFFSET = 2
SLIPPAGE = 0.25
PROFIT_TARGET_FIRST = 3

balance = BigDecimal(1000000)

positions = 0
stop_loss = 0.0
profit_target = BigDecimal(0)
available = true


def format_decimal(v)
  formatted_price = v.truncate(2).to_s("F")
end

def log_action(action, price, positions, balance, stop_loss=nil, profit_target=nil)
  positions += action

  updated_balance = balance + (action * -1.0 * price)

  msg = "-- #{action} @ #{format_decimal(price)}. Position = #{positions}. Balance = #{format_decimal(updated_balance)}"

  if stop_loss
    msg += ". Stop Loss = #{format_decimal(stop_loss)}"
  end

  if profit_target
    msg += ". Profit Target = #{format_decimal(profit_target)}"
  end

  puts msg

  return [positions, updated_balance]
end


data.group_by{ |row| row["date"] }.each do |date_str, rows|
  range_high, range_low = \
    BigDecimal(rows[0]["high"]), BigDecimal(rows[0]["low"])

  puts "#{date_str}. #{format_decimal(range_high)}-#{format_decimal(range_low)}"

  strategy_trading_window = rows.drop(1).select{ |row| row["rth"] == "TRUE" }

  if strategy_trading_window.length == 0
    next
  end

  strategy_trading_window.each do |row|
    _close = BigDecimal(row["close"])
    _low = BigDecimal(row["low"])
    _high = BigDecimal(row["high"])

    if available == false
      if (range_low <= _close) and (_close <= range_high)
        available = true
      end
    end

    #
    # long/short entry
    #
    if _close > range_high and positions < 2 and available
      price = _close + SLIPPAGE
      stop_loss = BigDecimal(_low) - STOP_LOSS_OFFSET
      profit_target = price + BigDecimal(PROFIT_TARGET_FIRST) if profit_target == 0
      positions, balance = log_action(1, price, positions, balance, stop_loss, profit_target)
    end
    if _close < range_low and positions > -2 and available
      price = _close - SLIPPAGE
      stop_loss = BigDecimal(_high) + STOP_LOSS_OFFSET
      profit_target = price - BigDecimal(PROFIT_TARGET_FIRST) if profit_target == 0
      positions, balance = log_action(-1, price, positions, balance, stop_loss, profit_target)
    end

    #
    # long/short stop loss exits
    #
    if positions > 0 and _close <= stop_loss
      price = _close - SLIPPAGE
      positions, balance = log_action(-positions, price, positions, balance)
      puts "Loss. #{format_decimal(balance)}"
      profit_target = 0.0
      available = false
    end
    if positions < 0 and _close > stop_loss
      price = _close + SLIPPAGE
      positions, balance = log_action(-positions, price, positions, balance)
      puts "Loss. #{format_decimal(balance)}"
      profit_target = 0.0
      available = false
    end

    #
    # long/short profit taking
    #
    if positions > 0 and _close >= profit_target
      price = _close - SLIPPAGE
      positions, balance = log_action(-positions, price, positions, balance)
      puts "Profit. #{format_decimal(balance)}"
      profit_target = 0.0
      available = false
    end

    if positions < 0 and _close <= profit_target
      price = _close + SLIPPAGE
      positions, balance = log_action(-positions, price, positions, balance)
      puts "Profit. #{format_decimal(balance)}"
      profit_target = 0.0
      available = false
    end
  end

  puts "End of day positions = #{positions}"

  row = strategy_trading_window[-1]
  _close = BigDecimal(row["close"])

  if positions > 0
    price = _close - SLIPPAGE
    positions, balance = log_action(-positions, price, positions, balance)
  elsif positions < 0
    price = _close + SLIPPAGE
    positions, balance = log_action(-positions, price, positions, balance)
  end

  puts "Close out positions = #{positions}"
end

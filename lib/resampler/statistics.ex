defmodule Resampler.Statistics do
  def aggregate(left, bucket) do
    {:ok, dt} = DateTime.from_unix(left)
    timestring = timestring(dt)

    [
      timestring, ",",
      Float.to_string(max(bucket)), ",",
      Float.to_string(median(bucket)), ",",
      Float.to_string(percentile(bucket, 75)), ",",
      Float.to_string(percentile(bucket, 25)), ",",
      Float.to_string(min(bucket)), "\n",
    ]
  end

  def nil_row(left) do
    {:ok, dt} = DateTime.from_unix(left)
    timestring = timestring(dt)
    "#{timestring},,,,,\n"
  end

  defp timestring(time) do
    time
    |> DateTime.to_iso8601
    |> String.replace("T", " ")
    |> String.replace("Z", "")
  end

  # adapted from: https://github.com/msharp/elixir-statistics/blob/master/lib/statistics.ex
  def mean(list) when is_list(list), do: do_mean(list, 0, 0)
  defp do_mean([], 0, 0), do: nil
  defp do_mean([], t, l), do: t / l
  defp do_mean([x|xs], t, l) do
    do_mean(xs, t + x, l + 1)
  end

  def max([]), do: nil
  def max(list) do
    Enum.max(list)
  end

  def min([]), do: nil
  def min(list) do
    Enum.min(list)
  end

  def median([]), do: nil
  def median(list) when is_list(list) do
    do_median list
  end
  defp do_median(mlist) do
    list = Enum.sort mlist
    middle = (length(list) - 1) / 2
    do_median(list, middle, :erlang.trunc(middle))
  end
  defp do_median(sorted_list, m, f) when m > f do
    sorted_list |> Enum.slice(f, 2) |> mean
  end
  defp do_median(sorted_list, _, f) do
    sorted_list |> Enum.at(f)
  end

  def percentile([], _), do: nil
  def percentile([v], _), do: v
  def percentile(list, 0), do: min(list)
  def percentile(list, 100), do: max(list)
  def percentile(list, n) when is_list(list) and is_number(n) do
    s = Enum.sort(list)
    r = n/100.0 * (length(list) - 1)
    f = :erlang.trunc(r)
    lower = Enum.at(s, f)
    upper = Enum.at(s, f + 1)
    lower + (upper - lower) * (r - f)
  end
end

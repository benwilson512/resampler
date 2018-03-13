defmodule Resampler.Statistics do
  def aggregate(ts, bucket) do
    {
      ts,
      max(bucket),
      median(bucket),
      percentile(bucket, 75),
      percentile(bucket, 25),
      min(bucket)
    }
  end

  def from_prev_row(ts, {_, max, median, p75, p25, min}) do
    {ts, max, median, p75, p25, min}
  end

  def nil_row(left) do
    {left, nil, nil, nil, nil, nil}
  end

  def to_csv({ts, max, median, p75, p25, min}) do
    {:ok, dt} = DateTime.from_unix(ts)
    timestring = timestring(dt)

    [
      timestring,
      ",",
      to_string(max),
      ",",
      to_string(median),
      ",",
      to_string(p75),
      ",",
      to_string(p25),
      ",",
      to_string(min),
      "\n"
    ]
  end

  defp timestring(time) do
    time
    |> DateTime.to_iso8601()
    |> String.replace("T", " ")
    |> String.replace("Z", "")
  end

  # adapted from: https://github.com/msharp/elixir-statistics/blob/master/lib/statistics.ex
  def mean(list) when is_list(list), do: do_mean(list, 0, 0)
  defp do_mean([], 0, 0), do: nil
  defp do_mean([], t, l), do: t / l

  defp do_mean([x | xs], t, l) do
    do_mean(xs, t + x, l + 1)
  end

  def max([]), do: nil

  def max(list) do
    Enum.max(list) * 1.0
  end

  def min([]), do: nil

  def min(list) do
    Enum.min(list) * 1.0
  end

  def median([]), do: nil

  def median(list) when is_list(list) do
    do_median(list)
  end

  defp do_median(mlist) do
    list = Enum.sort(mlist)
    middle = (length(list) - 1) / 2
    do_median(list, middle, :erlang.trunc(middle)) * 1.0
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
    r = n / 100.0 * (length(list) - 1)
    f = :erlang.trunc(r)
    lower = Enum.at(s, f)
    upper = Enum.at(s, f + 1)
    (lower + (upper - lower) * (r - f)) * 1.0
  end
end

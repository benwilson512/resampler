defmodule Resampler.Bucket do
  alias Resampler.Statistics

  def lazy(ts, width, opts) do
    case Enum.take(ts, 1) do
      [{start, _}] ->
        do_lazy(ts, width, start, opts)

      _ ->
        []
    end
  end

  defp do_lazy(ts, width, t, opts) do
    left = div(t, width) * width
    right = left + width
    buffer = []

    ts
    |> Stream.concat([:end])
    |> Stream.transform({left, right, width, buffer}, fn
      :end, {left, _right, _width, buffer} = acc ->
        {[Statistics.aggregate(left, buffer)], acc}

      {time, value}, {left, right, width, buffer} ->
        lazy_bucket(time, value, left, right, width, buffer)
    end)
    |> maybe_impute(left, opts[:impute])
  end

  defp maybe_impute(statistics, start, :prev) do
    nil_row = Statistics.nil_row(start)

    Stream.transform(statistics, nil_row, fn
      {ts, nil, nil, nil, nil, nil}, prev ->
        row = Statistics.from_prev_row(ts, prev)
        {[row], row}

      row, _prev ->
        {[row], row}
    end)
  end

  defp maybe_impute(statistics, _, _) do
    statistics
  end

  defp lazy_bucket(time, value, left, right, width, buffer) when time < right do
    buffer = [value | buffer]
    {[], {left, right, width, buffer}}
  end

  defp lazy_bucket(time, value, left, right, width, buffer) do
    result = [Statistics.aggregate(left, buffer)]

    left = left + width
    right = right + width

    {result, left, right} =
      cond do
        time > right ->
          fill(time, left, right, width, result)

        true ->
          {result, left, right}
      end

    {result, {left, right, width, [value]}}
  end

  defp fill(time, left, right, _width, acc) when time < right do
    {Enum.reverse(acc), left, right}
  end

  defp fill(time, left, right, width, acc) do
    acc = [Statistics.nil_row(left) | acc]
    fill(time, left + width, right + width, width, acc)
  end

  def eager([{left, _} | _] = ts, width, _opts \\ []) do
    left = div(left, width) * width
    buffer = []
    acc = []
    right = left + width
    do_bucket(ts, width, left, right, buffer, acc)
  end

  defp do_bucket([], _, _, _, [], acc), do: Enum.reverse(acc)

  defp do_bucket([], width, left, right, buffer, acc) do
    acc = [Statistics.aggregate(left, buffer) | acc]
    buffer = []
    do_bucket([], width, left, right, buffer, acc)
  end

  defp do_bucket([{t, v} | rest], width, left, right, buffer, acc) when t < right do
    buffer = [v | buffer]
    do_bucket(rest, width, left, right, buffer, acc)
  end

  defp do_bucket(list, width, left, right, buffer, acc) do
    acc = [Statistics.aggregate(left, buffer) | acc]
    left = left + width
    right = right + width
    buffer = []
    do_bucket(list, width, left, right, buffer, acc)
  end
end

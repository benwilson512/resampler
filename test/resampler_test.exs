defmodule ResamplerTest do
  use ExUnit.Case

  test "lazy is equivalent to eager with contiguous data" do
    ts = gen_ts(10, 3)

    eager_result = Resampler.eager(ts, 10)
    lazy_result = Resampler.lazy(ts, 10) |> Enum.to_list

    assert eager_result == lazy_result
  end

  test "boundaries" do
    outfile = File.stream!("output.csv")
    File.stream!("input.csv")
    |> Stream.map(fn row ->
      [t, v] = String.split(String.trim(row), ",")

      {String.to_integer(t), String.to_float(v)}
    end)
    |> Resampler.lazy(300)
    |> Resampler.to_csv
    |> Stream.into(outfile)
    |> Stream.run
  end

  test "lazy is equivalent to eager with missing data" do
    ts = gen_ts(10, 3, gaps: true)

    eager_result = Resampler.eager(ts, 10)
    lazy_result = Resampler.lazy(ts, 10) |> Enum.to_list

    assert eager_result == lazy_result
  end

  test "imputation prev works" do
    ts = gen_ts(10, 3, gaps: true)

    lazy_result = Resampler.lazy(ts, 10, impute: :prev) |> Enum.to_list
    expected = [
      {0, 8.0, 5.0, 6.0, 4.0, 2.0},
      {10, 8.0, 5.0, 6.0, 4.0, 2.0},
      {20, 8.0, 5.0, 6.0, 4.0, 2.0},
      {30, 8.0, 5.0, 6.0, 4.0, 2.0},
      {40, 8.0, 5.0, 6.0, 4.0, 2.0},
      {50, 8.0, 5.0, 6.0, 4.0, 2.0},
      {60, 8.0, 5.0, 6.0, 4.0, 2.0},
      {70, 8.0, 5.0, 6.0, 4.0, 2.0},
      {80, 8.0, 5.0, 6.0, 4.0, 2.0},
      {90, 8.0, 5.0, 6.0, 4.0, 2.0},
      {100, 109.0, 108.5, 109.0, 108.0, 107.0},
      {110, 113.0, 111.0, 112.0, 110.0, 110.0}
    ]

    assert expected == lazy_result
  end

  def gen_ts(num_points, num_sensors, opts \\ []) do
    for i <- 1..num_points, j <- 1..num_sensors do
      offset  = if opts[:gaps] && i > num_points / 2 do
        100
      else
        0
      end

      {i + j + offset, i + j + offset}
    end
    |> Enum.sort
  end
end

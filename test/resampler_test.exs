defmodule ResamplerTest do
  use ExUnit.Case

  test "lazy is equivalent to eager with contiguous data" do
    ts = gen_ts(10, 3)

    eager_result = Resample.eager(ts, 10)
    lazy_result = Resample.lazy(ts, 10) |> Enum.to_list

    assert eager_result == lazy_result
  end

  test "boundaries" do
    outfile = File.stream!("output.csv")
    File.stream!("input.csv")
    |> Stream.map(fn row ->
      [t, v] = String.split(String.strip(row), ",")

      {String.to_integer(t), String.to_float(v)}
    end)
    |> Resample.lazy(300)
    |> Stream.into(outfile)
    |> Stream.run
  end

  test "lazy is equivalent to eager with missing data" do
    ts = gen_ts(10, 3, gaps: true)

    eager_result = Resample.eager(ts, 10)
    lazy_result = Resample.lazy(ts, 10) |> Enum.to_list

    assert eager_result == lazy_result
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

defmodule Resampler do
  def eager(ts, width, opts \\ []) do
    Resampler.Bucket.eager(ts, width, opts)
  end

  def lazy(ts, width, opts \\ []) do
    Resampler.Bucket.lazy(ts, width, opts)
  end

  def to_csv(stats) do
    Stream.map(stats, &Resampler.Statistics.to_csv/1)
  end
end

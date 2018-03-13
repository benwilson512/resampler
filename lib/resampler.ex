defmodule Resampler do
  def eager(ts, width) do
    Resampler.Bucket.eager(ts, width)
  end

  def lazy(ts, width) do
    Resampler.Bucket.lazy(ts, width)
  end
end

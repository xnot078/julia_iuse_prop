using DataFramesMeta, DataFrames
using CSV
using PooledArrays
using Chain
using BenchmarkTools

# ================================================= #
# 1. 讀取table:
#     join tables:
#         y = fi_pl_detail.txt (使用性質) => fi_policy.txt |> groupby(id)
#         X = pb_corp_basic.txt (稅籍) => biz_corp_list.txt (公司登記)
# 2. encoding:

# ================================================= #
path = "D:\\test\\julia\\iuse_prop\\file\\ref\\tsic.csv"
CSV.read("D:\\test\\julia\\iuse_prop\\file\\ref\\tsic.csv", DataFrame; select = 1)
function load_table(path; selected = nothing)
    df = ifelse(selected != nothing,
           CSV.read(path, DataFrame; types = String),
           CSV.read(path, DataFrame; types = String, select = selected))

end

function to_numerical!(df::DataFrame; cols = [])
    check_parse(df::DataFrame, col::Union{String, Symbol}) = eltype(df[!, col]) <: String ? tryparse.(Int, df[!, col]) : df[!, col]
    for col in cols
        print(col, ", ")
        df[!, col] = check_parse(df, col)
    end
    print("...done.")
    return df
end
function to_PoolArray!(df::DataFrame; cols::Vector{T} where T<:Union{String, Symbol} = [])
    for col in cols
        df[!, col] = PooledArray(df[!, col]; compress = true)
    end
    return df
end

df = @chain path begin
    load_table(_)
    to_numerical!(_, cols = [:行業代號])
    to_PoolArray!(_, cols = [:版本別, :行業代號, :層級, :行業名稱])
end

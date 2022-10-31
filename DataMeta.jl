using DataFramesMeta, DataFrames
using CSV
using PooledArrays
using Chain
using BenchmarkTools
using StatsBase

# ================================================= #
# 1. 讀取table:
#     join tables:
#         y = fi_pl_detail.txt (使用性質) => fi_policy.txt |> groupby(id)
#         X = pb_corp_basic.txt (稅籍) => biz_corp_list.txt (公司登記)
# 2. encoding:

# ================================================= #
mutable struct DataSource
    name:: String
    path:: String
    data:: Union{DataFrame, Nothing}
    DataSource(name::String, path::String) = new(name, path)
end

function load_table(path; selected = nothing, delim:: String = ",")
    df = ifelse(selected != nothing,
           CSV.read(path, DataFrame; types = String, select = selected, delim = delim),
           CSV.read(path, DataFrame; types = String, delim = delim))
end
load_table(ds:: DataSource; selected = nothing, delim = ",") = load_table(ds.path; selected = selected, delim = delim)
load_table!(ds:: DataSource; selected = nothing, delim = ",") = begin ds.data = load_table(ds.path; selected = selected, delim = delim) end


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

# ====================================================================== #
# 建立稅務的階層表
# ====================================================================== #
function build_tsic_levels(path_tsicCsv:: String = "D:\\test\\julia\\iuse_prop\\file\\ref\\tsic.csv")
    tsic = DataSource("tax", path_tsicCsv)
    tsic.data = load_table(tsic; selected = [:行業代號, :層級, :行業名稱, :大業別])

    function _find_parent(population:: Vector{String}, child:: String)
        for i = 1:length(child)-1
            child[1:end-i] ∈ population && return child[1:end-1]
        end
        return missing
    end
    uq_data =  @chain tsic.data begin
        # @subset(tsic.data, :層級 .== "大")
        unique!(_)
        groupby(_, [:大業別, :行業代號])
        combine(_, nrow => :count,
                   :行業名稱 => (x->join(x, "|")) => :des)
        transform!(_, :行業代號 => (x->find_parent.([_[!, :行業代號]], x)) => :parent)
    end
    saved_path = replace(tsic.path, r"(.*)\\(.*.csv)" => s"\g<1>\\tsic_levels.csv")
    print("saved to: ")
    CSV.write(saved_path, uq_data; delim = "|")
end

# ---------------------------------------------------------------------- #
# 讀取tsic_level(i.e. 每個稅務分類上一層是誰)
# ---------------------------------------------------------------------- #
tsic_lvs = DataSource("tsic_lvs", "D:\\test\\julia\\iuse_prop\\file\\ref\\tsic_levels.csv")
load_table!(tsic_lvs, delim = "|")
Tsic_Parents = Dict(Pair.(tsic_lvs.data[!, :行業代號], tsic_lvs.data[!, :parent]))

# ---------------------------------------------------------------------- #
# 原始資料:
# 1. 外部處理:
#     稅務: 往上3層
# 2. 內部處理:
#     主要目標是內部的使用性質
# ---------------------------------------------------------------------- #

tax = DataSource("outer_tax", "D:\\test\\julia\\iuse_prop\\file\\ref\\biz_tax_list.txt")
load_table!(tax,
            selected = [:_id, :id, :tx_capital, :tx_ind_item],
            delim = "|")

using MLJ
coerce!(tax.data, :tx_ind_item => Multiclass)
schema(tax.data)

df = @chain path begin
    load_table(_)
    to_numerical!(_, cols = [:行業代號])
    to_PoolArray!(_, cols = [:版本別, :行業代號, :層級, :行業名稱])
end

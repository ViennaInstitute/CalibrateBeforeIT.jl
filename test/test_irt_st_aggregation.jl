using Test
using CalibrateBeforeIT

@testset "IRT_ST Aggregation Tests" begin

    @testset "_monthly_to_quarterly_mean: basic" begin
        # 3 months, one quarter
        monthly = [1.0, 2.0, 3.0]
        result = CalibrateBeforeIT._monthly_to_quarterly_mean(monthly)
        @test result == [2.0]
    end

    @testset "_monthly_to_quarterly_mean: multiple quarters" begin
        # 6 months, two quarters
        monthly = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
        result = CalibrateBeforeIT._monthly_to_quarterly_mean(monthly)
        @test result == [2.0, 5.0]
    end

    @testset "_monthly_to_quarterly_mean: length not multiple of 3 drops remainder" begin
        monthly = [1.0, 2.0, 3.0, 4.0]
        result = CalibrateBeforeIT._monthly_to_quarterly_mean(monthly)
        @test result == [2.0]
    end

    @testset "_in_sample_interp: no missing" begin
        v = [1.0, 2.0, 3.0, 4.0]
        out = CalibrateBeforeIT._in_sample_interp(v)
        @test out == [1.0, 2.0, 3.0, 4.0]
    end

    @testset "_in_sample_interp: interior missing" begin
        # missing at index 2, between 1.0 and 3.0 -> 2.0
        v = Union{Missing,Float64}[1.0, missing, 3.0]
        out = CalibrateBeforeIT._in_sample_interp(v)
        @test out == [1.0, 2.0, 3.0]
    end

    @testset "_in_sample_interp: leading missing clamps to first observed" begin
        v = Union{Missing,Float64}[missing, missing, 3.0, 4.0]
        out = CalibrateBeforeIT._in_sample_interp(v)
        @test out == [3.0, 3.0, 3.0, 4.0]
    end

    @testset "_in_sample_interp: trailing missing clamps to last observed" begin
        v = Union{Missing,Float64}[1.0, 2.0, missing, missing]
        out = CalibrateBeforeIT._in_sample_interp(v)
        @test out == [1.0, 2.0, 2.0, 2.0]
    end

    @testset "_in_sample_interp: all missing returns all missing" begin
        v = Union{Missing,Float64}[missing, missing]
        out = CalibrateBeforeIT._in_sample_interp(v)
        @test all(ismissing, out)
    end

    @testset "_gap_fill_quarterly: reported preserved" begin
        reported   = Union{Missing,Float64}[5.0, missing, 7.0]
        aggregated = [2.0, 4.0, 6.0]
        out = CalibrateBeforeIT._gap_fill_quarterly(reported, aggregated)
        @test out == [5.0, 4.0, 7.0]
    end

    @testset "_gap_fill_quarterly: all missing reported -> all aggregated" begin
        reported   = Union{Missing,Float64}[missing, missing, missing]
        aggregated = [2.0, 4.0, 6.0]
        out = CalibrateBeforeIT._gap_fill_quarterly(reported, aggregated)
        @test out == [2.0, 4.0, 6.0]
    end

    @testset "_gap_fill_quarterly: lengths must match" begin
        reported   = Union{Missing,Float64}[5.0, missing]
        aggregated = [2.0, 4.0, 6.0]
        @test_throws DimensionMismatch CalibrateBeforeIT._gap_fill_quarterly(reported, aggregated)
    end

    @testset "aggregate_irt_st_monthly_to_quarterly: end-to-end on synthetic parquet" begin
        using DuckDB
        using Tables

        # Set up a temp directory and override CalibrateBeforeIT.eurostat_path so pqfile() resolves here
        tmpdir = mktempdir()
        original_path = CalibrateBeforeIT.eurostat_path
        Core.eval(CalibrateBeforeIT, :(eurostat_path = $(tmpdir)))

        try
            conn = DBInterface.connect(DuckDB.DB())

            # Build irt_st_m.parquet: monthly IRT_M3 for a fake geo "ZZ"
            # 2018 Q1: months 1,2,3 with values 1,2,3 -> quarterly mean 2.0
            # 2018 Q2: months 4,5,6 with values 4,missing,6 -> interp 5 -> mean 5.0
            # (no quarterly row reported for Q2 -> gap-fill should pick 5.0)
            months = ["2018-01", "2018-02", "2018-03",
                      "2018-04", "2018-05", "2018-06"]
            mvals  = [1.0, 2.0, 3.0, 4.0, missing, 6.0]
            DBInterface.execute(conn, "CREATE TABLE m (freq VARCHAR, int_rt VARCHAR, geo VARCHAR, time VARCHAR, value DOUBLE)")
            values_clause_m = join(["('M','IRT_M3','ZZ','$(m)',$(ismissing(v) ? "NULL" : v))" for (m,v) in zip(months,mvals)], ",")
            DBInterface.execute(conn, "INSERT INTO m VALUES $(values_clause_m)")
            DBInterface.execute(conn, "COPY m TO '$(tmpdir)/irt_st_m.parquet' (FORMAT parquet)")

            # Build irt_st_q_raw.parquet: reported quarterly for ZZ at Q1 only (5.0), Q2 missing; plus an EA row to verify preservation
            DBInterface.execute(conn, "CREATE TABLE q (freq VARCHAR, int_rt VARCHAR, geo VARCHAR, time VARCHAR, value DOUBLE)")
            DBInterface.execute(conn, "INSERT INTO q VALUES ('Q','IRT_M3','ZZ','2018-Q1',5.0),('Q','IRT_M3','ZZ','2018-Q2',NULL),('Q','IRT_M3','EA','2018-Q1',-0.5)")
            DBInterface.execute(conn, "COPY q TO '$(tmpdir)/irt_st_q_raw.parquet' (FORMAT parquet)")

            # Run the gap-fill for geo ZZ over 2018..2018
            CalibrateBeforeIT.aggregate_irt_st_monthly_to_quarterly(
                conn; start_year=2018, end_year=2018, geos=["ZZ"], int_rt="IRT_M3")

            # Read back irt_st_q.parquet for ZZ
            res = CalibrateBeforeIT.execute_debug(conn,
                "SELECT time, value FROM '$(tmpdir)/irt_st_q.parquet' WHERE geo='ZZ' AND int_rt='IRT_M3' ORDER BY time")
            # The function fills the full quarter grid (Q1-Q4):
            #   Q1 reported=5.0 preserved
            #   Q2 gap-filled from monthly mean (4+5+6)/3=5.0
            #   Q3,Q4 gap-filled from monthly (trailing clamp to last observed 6.0)
            @test size(res, 1) == 4
            @test res.time[1] == "2018-Q1"
            @test res.value[1] == 5.0
            @test res.time[2] == "2018-Q2"
            @test res.value[2] == 5.0
            @test res.time[3] == "2018-Q3"
            @test res.value[3] == 6.0
            @test res.time[4] == "2018-Q4"
            @test res.value[4] == 6.0

            # EA row preserved untouched
            ea = CalibrateBeforeIT.execute_debug(conn,
                "SELECT value FROM '$(tmpdir)/irt_st_q.parquet' WHERE geo='EA' AND int_rt='IRT_M3' AND time='2018-Q1'")
            @test size(ea, 1) == 1
            @test ea.value[1] == -0.5
        finally
            Core.eval(CalibrateBeforeIT, :(eurostat_path = $(original_path)))
        end
    end

    @testset "aggregate_irt_st_monthly_to_quarterly: geo with no monthly data is preserved" begin
        using DuckDB

        tmpdir = mktempdir()
        original_path = CalibrateBeforeIT.eurostat_path
        Core.eval(CalibrateBeforeIT, :(eurostat_path = $(tmpdir)))

        try
            conn = DBInterface.connect(DuckDB.DB())

            # irt_st_m with NO rows for geo "YY" (zero monthly data)
            DBInterface.execute(conn, "CREATE TABLE m (freq VARCHAR, int_rt VARCHAR, geo VARCHAR, time VARCHAR, value DOUBLE)")
            DBInterface.execute(conn, "INSERT INTO m VALUES ('M','IRT_M3','ZZ','2018-01',1.0),('M','IRT_M3','ZZ','2018-02',2.0),('M','IRT_M3','ZZ','2018-03',3.0)")
            DBInterface.execute(conn, "COPY m TO '$(tmpdir)/irt_st_m.parquet' (FORMAT parquet)")

            # irt_st_q_raw: ZZ has reported quarterly; YY has reported quarterly that must be left untouched
            DBInterface.execute(conn, "CREATE TABLE q (freq VARCHAR, int_rt VARCHAR, geo VARCHAR, time VARCHAR, value DOUBLE)")
            DBInterface.execute(conn, "INSERT INTO q VALUES ('Q','IRT_M3','ZZ','2018-Q1',5.0),('Q','IRT_M3','YY','2018-Q1',9.0),('Q','IRT_M3','YY','2018-Q2',8.0)")
            DBInterface.execute(conn, "COPY q TO '$(tmpdir)/irt_st_q_raw.parquet' (FORMAT parquet)")

            # Run gap-fill for BOTH ZZ (has monthly) and YY (no monthly)
            CalibrateBeforeIT.aggregate_irt_st_monthly_to_quarterly(
                conn; start_year=2018, end_year=2018, geos=["ZZ", "YY"], int_rt="IRT_M3")

            # YY rows must be preserved as-is (spec: leave quarterly unchanged for zero-monthly geo)
            yy = CalibrateBeforeIT.execute_debug(conn,
                "SELECT time, value FROM '$(tmpdir)/irt_st_q.parquet' WHERE geo='YY' AND int_rt='IRT_M3' ORDER BY time")
            @test size(yy, 1) == 2
            @test yy.time[1] == "2018-Q1"
            @test yy.value[1] == 9.0
            @test yy.time[2] == "2018-Q2"
            @test yy.value[2] == 8.0

            # ZZ was gap-filled (Q1 reported=5.0 preserved, Q2-Q4 filled from monthly clamp=3.0)
            zz = CalibrateBeforeIT.execute_debug(conn,
                "SELECT time, value FROM '$(tmpdir)/irt_st_q.parquet' WHERE geo='ZZ' AND int_rt='IRT_M3' ORDER BY time")
            @test size(zz, 1) == 4
            @test zz.value[1] == 5.0  # reported preserved
        finally
            Core.eval(CalibrateBeforeIT, :(eurostat_path = $(original_path)))
        end
    end
end

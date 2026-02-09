#include <benchmark/benchmark.h>

#include "tsdb.hh"

#include <format>
#include <print>

struct Vec3 {
    f64 x, y, z;
};

template <>
struct std::formatter<Vec3> {
    constexpr auto parse(std::format_parse_context& ctx) {
        return ctx.begin();
    }

    auto format(const Vec3& v, std::format_context& ctx) const {
        return std::format_to(
            ctx.out(),
            "Vec3 {{ .x = {}, .y = {}, .z = {} }}",
            v.x, v.y, v.z
        );
    }
};









auto main() -> i32 {
    TSDB db {1};

    auto vec3_handle = db.register_struct(
    "Vec3", {
    	{"x", TSDB::F64},
    	{"y", TSDB::F64},
    	{"z", TSDB::F64},
    });

    db.insert(Vec3 { .x = 1, .y = 1, .z = 1}, vec3_handle);

    auto new_vec = db.query_first<Vec3>(vec3_handle);
    std::println("{}", new_vec);

    return 0;
}











// static void BM_RegisterStruct(benchmark::State& state) {
//     for (auto _ : state) {
//         TSDB db{1};
//         auto handle = db.register_struct(
//             "Vec3", {
//                 {"x", TSDB::F64},
//                 {"y", TSDB::F64},
//                 {"z", TSDB::F64},
//             });
//         benchmark::DoNotOptimize(handle);
//     }
// }
// BENCHMARK(BM_RegisterStruct);

// static void BM_Insert_Single(benchmark::State& state) {
//     TSDB db{1};
//     auto vec3_handle = db.register_struct(
//         "Vec3", {
//             {"x", TSDB::F64},
//             {"y", TSDB::F64},
//             {"z", TSDB::F64},
//         });
    
//     for (auto _ : state) {
//         db.insert(Vec3{1.0, 2.0, 3.0}, vec3_handle);
//     }
    
//     state.SetItemsProcessed(state.iterations());
// }
// BENCHMARK(BM_Insert_Single);

// static void BM_Insert_Bulk(benchmark::State& state) {
//     TSDB db{1};
//     auto vec3_handle = db.register_struct(
//         "Vec3", {
//             {"x", TSDB::F64},
//             {"y", TSDB::F64},
//             {"z", TSDB::F64},
//         });
    
//     const auto count = state.range(0);
    
//     for (auto _ : state) {
//         for (int i = 0; i < count; ++i) {
//             db.insert(Vec3 {
//                 static_cast<double>(i),
//                 static_cast<double>(i * 2),
//                 static_cast<double>(i * 3)
//             }, vec3_handle);
//         }
//     }
    
//     state.SetItemsProcessed(state.iterations() * count);
// }
// BENCHMARK(BM_Insert_Bulk)->Range(8, 8<<10);

// static void BM_Query_First(benchmark::State& state) {
//     TSDB db{1};
//     auto vec3_handle = db.register_struct(
//         "Vec3", {
//             {"x", TSDB::F64},
//             {"y", TSDB::F64},
//             {"z", TSDB::F64},
//         });
    
//     const auto count = state.range(0);
//     for (int i = 0; i < count; ++i) {
//         db.insert(Vec3 {
//             static_cast<double>(i),
//             static_cast<double>(i),
//             static_cast<double>(i)
//         }, vec3_handle);
//     }
    
//     for (auto _ : state) {
//         auto result = db.query_first<Vec3>(vec3_handle);
//         benchmark::DoNotOptimize(result);
//     }
// }
// BENCHMARK(BM_Query_First)->Range(8, 8<<10);

// static void BM_FullWorkflow(benchmark::State& state) {
//     for (auto _ : state) {
//         TSDB db{1};
//         auto vec3_handle = db.register_struct(
//             "Vec3", {
//                 {"x", TSDB::F64},
//                 {"y", TSDB::F64},
//                 {"z", TSDB::F64},
//             });
        
//         db.insert(Vec3{1.0, 2.0, 3.0}, vec3_handle);
//         auto result = db.query_first<Vec3>(vec3_handle);
//         benchmark::DoNotOptimize(result);
//     }
// }
// BENCHMARK(BM_FullWorkflow);

// BENCHMARK_MAIN();

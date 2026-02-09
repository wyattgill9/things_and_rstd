#include "absl/container/flat_hash_map.h"

#include "utils.hh"

#include <cstring>
#include <utility>
#include <vector>
#include <ranges>
#include <string>
#include <initializer_list>

template <std::unsigned_integral T>
[[nodiscard]] constexpr auto align_up(T value, T alignment) noexcept -> T {
    assert(alignment != 0);
    assert(std::has_single_bit(alignment));

    return (value + alignment - 1) & ~(alignment - 1);
}

struct TypeHandle {
public:
    constexpr TypeHandle(u32 v) : v(v) {}

    friend bool operator==(TypeHandle rhs, TypeHandle lhs) {
        return rhs.v == lhs.v;
    }

    template <typename H>
    friend H AbslHashValue(H h, const TypeHandle& t) {
        return H::combine(std::move(h), t.v);
    }

public:
    u32 v;
};

class Schema {
public:
    enum class TypeKind : u8 {
        U8, U16, U32, U64,
        I8, I16, I32, I64,
        F32, F64,
        BOOL,
        TIMESTAMP_NS,

        NUM_PRIMITIVES,

        STRUCT,
    };
        
    struct TypeMeta {
        std::string name_;
        TypeKind    kind_;
        size_t      size_;
        size_t      alignment_;

        std::vector<TypeHandle> fields_;
        std::vector<size_t>    field_offsets_;

        static constexpr auto primitive(
            std::string_view name,
            TypeKind          kind,
            size_t            size,
            size_t            alignment
        ) -> TypeMeta {
            return TypeMeta{
                .name_      = std::string{name},
                .kind_      = kind,
                .size_      = size,
                .alignment_ = alignment,
                .fields_        = {},
                .field_offsets_ = {},
            };
        }

        // one level of depth TODO -> flattening
        auto fields(std::vector<TypeMeta>& types) -> std::vector<TypeMeta> {
            return fields_
                | std::views::transform([&](TypeHandle& field_handle) { return types[field_handle.v]; })
                | std::ranges::to<std::vector<TypeMeta>>();
        }
    };

public:
    Schema(size_t est_num_types) {
        types_.reserve(est_num_types + std::to_underlying(TypeKind::NUM_PRIMITIVES));
        init_primitives(types_);
    }

    auto register_struct(std::string name,
                         std::initializer_list<std::pair<std::string, const TypeHandle>> fields) -> TypeHandle
    {           
        size_t struct_size      = 0;
        size_t struct_alignment = 1;

        std::vector<TypeHandle> struct_fields;
        std::vector<size_t>     field_offsets;

        for(auto&& [name, type] : fields) {
            const TypeMeta& meta = meta_of(type);

            struct_alignment = std::max(struct_alignment, meta.alignment_);
            struct_size      = align_up(struct_size     , meta.alignment_);

            field_offsets.push_back(struct_size);
            struct_size += meta.size_;
            struct_fields.push_back(type);
        }

        struct_size = align_up(struct_size, struct_alignment);

        const TypeHandle handle = types_.size();

        types_.push_back(TypeMeta {
            .name_          = std::move(name),
            .kind_          = TypeKind::STRUCT,
            .size_          = struct_size,
            .alignment_     = struct_alignment,
            .fields_        = std::move(struct_fields),
            .field_offsets_ = std::move(field_offsets),
        });

        return handle;
    }

    [[nodiscard]] inline auto meta_of (const TypeHandle& type) const -> const TypeMeta& { return types_[type.v];           }
    [[nodiscard]] inline auto kind_of (const TypeHandle& type) const -> const TypeKind& { return meta_of(type).kind_;      }
    [[nodiscard]] inline auto size_of (const TypeHandle& type) const -> const size_t&   { return meta_of(type).size_;      }
    [[nodiscard]] inline auto align_of(const TypeHandle& type) const -> const size_t&   { return meta_of(type).alignment_; }

private:
    auto init_primitives(std::vector<TypeMeta>& types) -> void {
        constexpr std::array<TypeMeta, std::to_underlying(TypeKind::NUM_PRIMITIVES)>
        primitives {{
            TypeMeta::primitive("u8",  TypeKind::U8,  1, 1),
            TypeMeta::primitive("u16", TypeKind::U16, 2, 2),
            TypeMeta::primitive("u32", TypeKind::U32, 4, 4),
            TypeMeta::primitive("u64", TypeKind::U64, 8, 8),

            TypeMeta::primitive("i8",  TypeKind::I8,  1, 1),
            TypeMeta::primitive("i16", TypeKind::I16, 2, 2),
            TypeMeta::primitive("i32", TypeKind::I32, 4, 4),
            TypeMeta::primitive("i64", TypeKind::I64, 8, 8),

            TypeMeta::primitive("f32", TypeKind::F32, 4, 4),
            TypeMeta::primitive("f64", TypeKind::F64, 8, 8),

            TypeMeta::primitive("bool", TypeKind::BOOL, 1, 1),
            TypeMeta::primitive("timestamp_ns", TypeKind::TIMESTAMP_NS, 8, 8),
        }};

        types.insert(types.end(), primitives.begin(), primitives.end());
    }

    std::vector<TypeMeta> types_;  
};

struct Column {
public:
    Column() = default;

    auto push(const std::byte* data, size_t size) -> void {
        data_.insert(data_.end(), data, data + size);
    }

    [[nodiscard]] auto at(size_t row, size_t elem_size) const -> const std::byte* {
        return data_.data() + row * elem_size;
    }

    [[nodiscard]] auto row_count(size_t elem_size) const -> size_t {
        return elem_size > 0 ? data_.size() / elem_size : 0;
    }

private:
    std::vector<std::byte> data_;
};

struct Table {
public:
    Table(size_t num_columns) : columns_(num_columns) {}

    auto insert_row(const std::byte* src,
                    const std::vector<size_t>& offsets,
                    const std::vector<size_t>& sizes) -> void
    {
        for (size_t i = 0; i < columns_.size(); ++i) {
            columns_[i].push(src + offsets[i], sizes[i]);
        }
        ++row_count_;
    }

    auto read_row(size_t row, std::byte* dst,
                  const std::vector<size_t>& offsets,
                  const std::vector<size_t>& sizes) const -> void
    {
        for (size_t i = 0; i < columns_.size(); ++i) {
            std::memcpy(dst + offsets[i], columns_[i].at(row, sizes[i]), sizes[i]);
        }
    }

    [[nodiscard]] auto row_count() const -> size_t { return row_count_; }

private:
    size_t row_count_ = 0;
    std::vector<Column> columns_;
};

class TSDB {
public:
    TSDB(size_t est_num_types = 1) : schema_(est_num_types) {}

    ~TSDB()           = default;
    TSDB(const TSDB&) = delete;
    TSDB(TSDB&&)      = delete;

    auto register_struct(std::string name,
                         std::initializer_list<std::pair<std::string, const TypeHandle>> fields) -> TypeHandle
    {
        return schema_.register_struct(name, fields);
    }

    template<typename T>
    auto insert(const T& src, TypeHandle type) -> void {
        static_assert(std::is_trivially_copyable_v<T>);

        Table& table = get_or_create_table(type);
        const auto& meta = schema_.meta_of(type);
        const auto* bytes = reinterpret_cast<const std::byte*>(&src);

        auto field_sizes = get_field_sizes(meta);
        table.insert_row(bytes, meta.field_offsets_, field_sizes);
    }

    template<typename T>
    [[nodiscard]] auto query_first(TypeHandle type) -> T {
        static_assert(std::is_trivially_copyable_v<T>);

        Table& table = get_or_create_table(type);
        const auto& meta = schema_.meta_of(type);

        T result{};
        if (table.row_count() == 0) return result;

        auto* dst = reinterpret_cast<std::byte*>(&result);
        auto field_sizes = get_field_sizes(meta);
        table.read_row(0, dst, meta.field_offsets_, field_sizes);

        return result;
    }

    // Default Types
    constexpr static TypeHandle U8  = 0;
    constexpr static TypeHandle U16 = 1;
    constexpr static TypeHandle U32 = 2;
    constexpr static TypeHandle U64 = 3;
    constexpr static TypeHandle I8  = 4;
    constexpr static TypeHandle I16 = 5;
    constexpr static TypeHandle I32 = 6;
    constexpr static TypeHandle I64 = 7;
    constexpr static TypeHandle F32 = 8;   
    constexpr static TypeHandle F64 = 9;
    constexpr static TypeHandle TIMESTAMP_NS = 10;

private:
    [[nodiscard]] auto get_or_create_table(TypeHandle type) -> Table& {
        const auto& meta = schema_.meta_of(type);
        return tables_.try_emplace(type, meta.fields_.size()).first->second;
    }   

    [[nodiscard]] auto get_field_sizes(const Schema::TypeMeta& meta) const -> std::vector<size_t> {
        return meta.fields_
            | std::views::transform([this](auto&& field) { return schema_.size_of(field); })
            | std::ranges::to<std::vector<size_t>>();       
    }

    Schema schema_;
    absl::flat_hash_map<TypeHandle, Table> tables_;   
};

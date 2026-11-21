#pragma once
#include <arrow/io/file.h>
#include <parquet/column_writer.h>
#include <parquet/file_writer.h>
#include <algorithm>
#include <filesystem>
#include <optional>
#include <string_view>
#include <vector>

namespace shopping::output {

using Type = parquet::Type;
using Converted = parquet::ConvertedType;
struct Field {
    const char* name;
    Type::type type;
    Converted::type logical = Converted::NONE;
    bool optional = false;
};

inline parquet::schema::NodeVector schema_fields(bool users) {
    std::vector<Field> fields;
    if (users) {
        fields = {{"user_id", Type::BYTE_ARRAY, Converted::UTF8}, {"day", Type::INT32, Converted::DATE},
                  {"sessions", Type::INT64}, {"events", Type::INT64}, {"revenue_cents", Type::INT64},
                  {"top_category", Type::BYTE_ARRAY, Converted::UTF8, true}};
    } else {
        fields = {{"campaign_id", Type::BYTE_ARRAY, Converted::UTF8}, {"channel", Type::BYTE_ARRAY, Converted::UTF8},
                  {"day", Type::INT32, Converted::DATE}, {"sessions", Type::INT64},
                  {"purchasing_sessions", Type::INT64}, {"revenue_cents", Type::INT64}, {"assisted_sessions", Type::INT64}};
    }
    parquet::schema::NodeVector nodes;
    for (auto f : fields) {
        nodes.push_back(parquet::schema::PrimitiveNode::Make(f.name,
            f.optional ? parquet::Repetition::OPTIONAL : parquet::Repetition::REQUIRED, f.type, f.logical));
    }
    return nodes;
}

class Batch {
    parquet::RowGroupWriter* group_;
    size_t count_;
public:
    Batch(parquet::RowGroupWriter* group, size_t count) : group_(group), count_(count) {}
    template<typename ParquetType, class Get>
    void values(Get get) {
        std::vector<typename ParquetType::c_type> buffer;
        buffer.reserve(count_);
        for (size_t i = 0; i < count_; ++i) buffer.push_back(get(i));
        auto* col = static_cast<parquet::TypedColumnWriter<ParquetType>*>(group_->NextColumn());
        col->WriteBatch(count_, nullptr, nullptr, buffer.data());
    }
    template<class Get>
    void strings(Get get) {
        values<parquet::ByteArrayType>([&](size_t i) { return parquet::ByteArray(get(i)); });
    }
    template<class Get>
    void nullable_strings(Get get) {
        std::vector<int16_t> levels(count_);
        std::vector<parquet::ByteArray> data;
        data.reserve(count_);
        for (size_t i = 0; i < count_; ++i) {
            std::optional<std::string_view> value = get(i);
            if (value) { levels[i] = 1; data.emplace_back(*value); }
        }
        auto* col = static_cast<parquet::ByteArrayWriter*>(group_->NextColumn());
        col->WriteBatch(count_, levels.data(), nullptr, data.data());
    }
};

template<class Write>
void write(const std::filesystem::path& path, bool users, size_t rows, Write fill) {
    const auto temporary = path.parent_path() / ("." + path.filename().string() + ".tmp");
    auto opened = arrow::io::FileOutputStream::Open(temporary.string());
    if (!opened.ok()) throw std::runtime_error(opened.status().ToString());
    auto stream = std::move(opened).ValueOrDie();
    auto schema = std::static_pointer_cast<parquet::schema::GroupNode>(
        parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, schema_fields(users)));
    parquet::WriterProperties::Builder settings;
    settings.disable_dictionary()->disable_statistics();
    auto file = parquet::ParquetFileWriter::Open(stream, schema, settings.build());
    constexpr size_t group_size = 65536;
    for (size_t offset = 0; offset < rows; offset += group_size) {
        size_t count = std::min(group_size, rows - offset);
        auto* group = file->AppendRowGroup();
        Batch batch(group, count);
        fill(batch, offset);
        group->Close();
    }
    file->Close();
    auto status = stream->Close();
    if (!status.ok()) throw std::runtime_error(status.ToString());
    std::filesystem::rename(temporary, path);
}

} // namespace shopping::output

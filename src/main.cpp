#define BLT_DISABLE_TRACE
#include <filesystem>
#include <blt/fs/loader.h>
#include <blt/logging/logging.h>
#include <blt/logging/status.h>
#include <blt/parse/argparse_v2.h>
#include <blt/std/hashmap.h>

enum class case_t
{
	UPPERCASE, LOWERCASE
};

blt::hashset_t<std::string> sqlite_keywords{
	"ABORT",
	"ACTION",
	"ADD",
	"AFTER",
	"ALL",
	"ALTER",
	"ALWAYS",
	"ANALYZE",
	"AND",
	"AS",
	"ASC",
	"ATTACH",
	"AUTOINCREMENT",
	"BEFORE",
	"BEGIN",
	"BETWEEN",
	"BY",
	"CASCADE",
	"CASE",
	"CAST",
	"CHECK",
	"COLLATE",
	"COLUMN",
	"COMMIT",
	"CONFLICT",
	"CONSTRAINT",
	"CREATE",
	"CROSS",
	"CURRENT",
	"CURRENT_DATE",
	"CURRENT_TIME",
	"CURRENT_TIMESTAMP",
	"DATABASE",
	"DEFAULT",
	"DEFERRABLE",
	"DEFERRED",
	"DELETE",
	"DESC",
	"DETACH",
	"DISTINCT",
	"DO",
	"DROP",
	"EACH",
	"ELSE",
	"END",
	"ESCAPE",
	"EXCEPT",
	"EXCLUDE",
	"EXCLUSIVE",
	"EXISTS",
	"EXPLAIN",
	"FAIL",
	"FILTER",
	"FIRST",
	"FOLLOWING",
	"FOR",
	"FOREIGN",
	"FROM",
	"FULL",
	"GENERATED",
	"GLOB",
	"GROUP",
	"GROUPS",
	"HAVING",
	"IF",
	"IGNORE",
	"IMMEDIATE",
	"IN",
	"INDEX",
	"INDEXED",
	"INITIALLY",
	"INNER",
	"INSERT",
	"INSTEAD",
	"INTERSECT",
	"INTO",
	"IS",
	"ISNULL",
	"JOIN",
	"KEY",
	"LAST",
	"LEFT",
	"LIKE",
	"LIMIT",
	"MATCH",
	"NATURAL",
	"NO",
	"NOT",
	"NOTHING",
	"NOTNULL",
	"NULL",
	"NULLS",
	"OF",
	"OFFSET",
	"ON",
	"OR",
	"ORDER",
	"OTHERS",
	"OUTER",
	"OVER",
	"PARTITION",
	"PLAN",
	"PRAGMA",
	"PRECEDING",
	"PRIMARY",
	"QUERY",
	"RAISE",
	"RANGE",
	"RECURSIVE",
	"REFERENCES",
	"REGEXP",
	"REINDEX",
	"RELEASE",
	"RENAME",
	"REPLACE",
	"RESTRICT",
	"RIGHT",
	"ROLLBACK",
	"ROW",
	"ROWS",
	"SAVEPOINT",
	"SELECT",
	"SET",
	"TABLE",
	"TEMP",
	"TEMPORARY",
	"THEN",
	"TIES",
	"TO",
	"TRANSACTION",
	"TRIGGER",
	"UNBOUNDED",
	"UNION",
	"UNIQUE",
	"UPDATE",
	"USING",
	"VACUUM",
	"VALUES",
	"VIEW",
	"VIRTUAL",
	"WHEN",
	"WHERE",
	"WINDOW",
	"WITH",
	"WITHOUT",
	"RETURNING"
};

blt::logging::status_progress_bar_t progress;

double number_of_files = 1;
double processed_files = 0;

void process_file(std::optional<std::string> search, const std::string& file, case_t c)
{
	auto file_data = blt::fs::getFile(file);
	size_t last_pos = 0;
	size_t pos = 0;

	if (!search)
		search = "\"";

	while ((pos = file_data.find(*search, last_pos)) != std::string::npos)
	{
		const auto begin = file_data.find('"', pos);
		size_t end = begin;
		for (size_t i = begin + 1; i < file_data.size(); i++)
		{
			if (file_data[i] == '"' && file_data[i - 1] != '\\')
			{
				end = i;
				break;
			}
		}
		if (end == begin)
		{
			BLT_WARN("Unable to process SQL statement, at char pos {}, local string looks like '{}'", pos, std::string_view(
				file_data.data() + std::max(static_cast<blt::i64>(pos) - 10, 0l), std::min(20ul, file_data.size() - pos - 10)));
			last_pos = end + 1;
			continue;
		}

		size_t space_pos = file_data.find(' ', begin);
		size_t last_space = space_pos;

		auto substr = blt::string::toUpperCase(file_data.substr(begin + 1, space_pos - begin - 1));
		BLT_TRACE("Found substring '{}' at {} until {}", substr, begin + 1, space_pos - 1);

		if (sqlite_keywords.contains(substr))
		{
			switch (c)
			{
				case case_t::UPPERCASE:
					file_data.replace(begin + 1, substr.size(), substr);
				break;
				case case_t::LOWERCASE:
					file_data.replace(begin + 1, substr.size(), blt::string::toLowerCase(substr));
				break;
			}
		}

		while (((space_pos = file_data.find(' ', last_space + 1)) != std::string::npos) && space_pos < end)
		{
			substr = blt::string::toUpperCase(file_data.substr(last_space + 1, space_pos - last_space - 1));
			BLT_TRACE("Found substring '{}' at {} until {}", substr, last_space + 1, last_space + 1 + substr.size());

			if (sqlite_keywords.contains(substr))
			{
				switch (c)
				{
					case case_t::UPPERCASE:
						file_data.replace(last_space + 1, substr.size(), substr);
						break;
					case case_t::LOWERCASE:
						file_data.replace(last_space + 1, substr.size(), blt::string::toLowerCase(substr));
						break;
				}
			}

			last_space = space_pos;
		}

		substr = blt::string::toUpperCase(file_data.substr(last_space + 1, end - last_space - 1));
		BLT_TRACE("Found substring '{}' at {} until {}", substr, last_space + 1, last_space + 1 + substr.size());

		if (sqlite_keywords.contains(substr))
		{
			switch (c)
			{
				case case_t::UPPERCASE:
					file_data.replace(last_space + 1, substr.size(), substr);
				break;
				case case_t::LOWERCASE:
					file_data.replace(last_space + 1, substr.size(), blt::string::toLowerCase(substr));
				break;
			}
		}

		BLT_DEBUG("Processed SQL statement at pos {}", pos);
		last_pos = end + 1;
	}
	++processed_files;
	progress.set_progress(processed_files / number_of_files);
	std::ofstream out(file, std::ios::out);
	out.write(file_data.data(), static_cast<ssize_t>(file_data.size()));
	BLT_INFO("Processed file {}", file);
}

void process_directory(const std::optional<std::string>& search, const std::string& dir, case_t c)
{
	std::vector<std::string> files_to_process;
	std::vector<std::string> directories_to_process;
	directories_to_process.push_back(dir);
	while (!directories_to_process.empty())
	{
		const auto local_dir = directories_to_process.back();
		directories_to_process.pop_back();
		BLT_DEBUG("Processing directory '{}'", local_dir);
		for (auto& file : std::filesystem::directory_iterator(local_dir))
		{
			if (file.is_directory())
				directories_to_process.push_back(file.path().string());
			else
				files_to_process.push_back(file.path().string());
		}
	}
	number_of_files = static_cast<double>(files_to_process.size());
	for (const auto& file : files_to_process)
		process_file(search, file, c);
}

int main(const int argc, const char** argv)
{
	blt::logging::status_bar_t status;
	blt::logging::get_global_config().add_injector(status.add(progress));

	blt::argparse::argument_parser_t parser{"Rename SQL statements to be of a case."};
	parser.with_help();
	parser.with_version();
	parser.add_flag("-r", "--recursive").set_dest("-r").make_flag().set_help("Treat path as a directory and recursively iterate through it.");
	parser.add_flag("-u", "--uppercase").set_dest("-u").make_flag().set_help("Make SQL statements uppercase. This is the default option");
	parser.add_flag("-l", "--lowercase").set_dest("-l").make_flag().set_help("Make SQL statements lowercase.");
	parser.add_positional("path").set_help("Path to the file or directory to process.");
	parser.add_positional("search").set_help(
		"Search string for parser to lock onto for SQL replacement. "
		"This should be everything BEFORE the first \" (Do not include the double quote)").set_required(false);

	const auto args = parser.parse(argc, argv);

	if (args.get<bool>("-u") && args.get<bool>("-l"))
	{
		BLT_ERROR("Cannot use both uppercase and lowercase flags at the same time");
		return 1;
	}

	auto c = case_t::UPPERCASE;

	if (args.get<bool>("-u"))
		c = case_t::UPPERCASE;
	if (args.get<bool>("-l"))
		c = case_t::LOWERCASE;

	BLT_INFO("Running on path '{}'", args.get("path"));

	std::optional<std::string> search;
	if (args.contains("search"))
		search = args.get("search");

	if (args.get<bool>("-r"))
		process_directory(search, args.get("path"), c);
	else
		process_file(search, args.get("path"), c);
}

# Removing Deprecated Options from RocksDB
### Keep in these files:
- [ ] **KEEP** entry in type_info (`options/cf_options.cc` or `options/db_options.cc`) with `OptionVerificationType::kDeprecated` for loading old option files
- [ ] **KEEP** or add test in `options/options_test.cc` `OptionsOldApiTest::GetOptionsFromMapTest` for loading old option files

### Documentation:
- [ ] Add release note to `HISTORY.md` and `unreleased_history/`

<?php

namespace mgboot\dal\db;

use DateTime;
use Illuminate\Support\Collection;
use mgboot\common\Cast;
use mgboot\common\constant\DateTimeFormat;
use mgboot\common\constant\Regexp;
use mgboot\common\util\ArrayUtils;
use mgboot\common\util\StringUtils;

final class QueryBuilder
{
    /**
     * @var string
     */
    private $tableName;

    /**
     * @var array
     */
    private $fields = [];

    /**
     * @var array
     */
    private $joins = [];

    /**
     * @var array
     */
    private $conditions = [];

    /**
     * @var array
     */
    private $params = [];

    /**
     * @var array
     */
    private $orderBy = [];

    /**
     * @var array
     */
    private $groupBy = [];

    /**
     * @var array
     */
    private $limits = [];

    private function __construct(string $tableName = '')
    {
        $this->tableName = $tableName;
    }

    public static function create(string $tableName = ''): self
    {
        return new self($tableName);
    }

    public function table(string $tableName): self
    {
        $this->tableName = $this->getFullTableNameOrFullFieldName($tableName);
        return $this;
    }

    /**
     * @param string|string[] $fields
     * @return QueryBuilder
     */
    public function fields($fields): self
    {
        if (is_string($fields)) {
            if ($fields === '*') {
                $this->fields = [];
                return $this;
            }

            $fields = preg_split(Regexp::COMMA_SEP, $fields);
        }

        if (!is_array($fields) || empty($fields)) {
            return $this;
        }

        foreach ($fields as $field) {
            $idx = $this->findFieldIndex($field);

            if ($idx < 0) {
                $this->fields[] = $this->getFullTableNameOrFullFieldName($field);
            } else {
                $this->fields[$idx] = $this->getFullTableNameOrFullFieldName($field);
            }
        }

        return $this;
    }

    /**
     * @param string $tableName
     * @param string|Expression ...$args
     * @return QueryBuilder
     */
    public function join(string $tableName, ...$args): self
    {
        $type = 'inner';
        $on = '';
        $cnt = count($args);

        if ($cnt === 1 && is_string($args[0])) {
            $on = $args[0];
        } else if ($cnt === 2 && is_string($args[0]) && is_string($args[1])) {
            $on = $args[0];
            $type = $args[1];
        } else if ($cnt === 3 && is_string($args[1])) {
            $sb = [];
            $p0 = $args[0];

            if (is_string($p0)) {
                $sb[] = $p0;
            } else if ($p0 instanceof Expression) {
                $sb[] = $p0->getExpr();
            }

            $p2 = $args[2];

            if (is_string($p2)) {
                $sb[] = $p2;
            } else if ($p2 instanceof Expression) {
                $sb[] = $p2->getExpr();
            }

            if (count($sb) === 2) {
                $p1 = sprintf(' %s ', trim($args[1]));
                $on = implode($p1, $sb);
            }
        } else if ($cnt > 3 && is_string($args[1]) && is_string($args[3])) {
            $type = trim($args[3]);
            $sb = [];
            $p0 = $args[0];

            if (is_string($p0)) {
                $sb[] = $p0;
            } else if ($p0 instanceof Expression) {
                $sb[] = $p0->getExpr();
            }

            $p2 = $args[2];

            if (is_string($p2)) {
                $sb[] = $p2;
            } else if ($p2 instanceof Expression) {
                $sb[] = $p2->getExpr();
            }

            if (count($sb) === 2) {
                $p1 = sprintf(' %s ', trim($args[1]));
                $on = implode($p1, $sb);
            }
        }

        if ($on === '') {
            return $this;
        }

        $type = strtoupper($type);
        $tableName = $this->getFullTableNameOrFullFieldName($tableName);
        $this->joins[] = "$type JOIN $tableName ON $on";
        return $this;
    }

    /**
     * @param string $tableName
     * @param string|Expression ...$args
     * @return QueryBuilder
     */
    public function outerJoin(string $tableName, ...$args): self
    {
        if (empty($args)) {
            return $this;
        }

        $args[] = 'outer';
        return $this->join($tableName, ...$args);
    }

    /**
     * @param string $tableName
     * @param string|Expression ...$args
     * @return QueryBuilder
     */
    public function leftJoin(string $tableName, ...$args): self
    {
        if (empty($args)) {
            return $this;
        }

        $args[] = 'left';
        return $this->join($tableName, ...$args);
    }

    /**
     * @param string $tableName
     * @param string|Expression ...$args
     * @return QueryBuilder
     */
    public function rightJoin(string $tableName, ...$args): self
    {
        if (empty($args)) {
            return $this;
        }

        $args[] = 'right';
        return $this->join($tableName, ...$args);
    }

    /**
     * @param string $tableName
     * @param string|Expression ...$args
     * @return QueryBuilder
     */
    public function crossJoin(string $tableName, ...$args): self
    {
        if (empty($args)) {
            return $this;
        }

        $args[] = 'cross';
        return $this->join($tableName, ...$args);
    }

    /**
     * @param string $tableName
     * @param string|Expression ...$args
     * @return QueryBuilder
     */
    public function leftOuterJoin(string $tableName, ...$args): self
    {
        if (empty($args)) {
            return $this;
        }

        $args[] = 'left outer';
        return $this->join($tableName, ...$args);
    }

    /**
     * @param string $tableName
     * @param string|Expression ...$args
     * @return QueryBuilder
     */
    public function rightOuterJoin(string $tableName, ...$args): self
    {
        if (empty($args)) {
            return $this;
        }

        $args[] = 'right outer';
        return $this->join($tableName, ...$args);
    }

    /**
     * @param string|Expression $field
     * @param mixed ...$args
     * @return QueryBuilder
     */
    public function where($field, ...$args): self
    {
        if (empty($args)) {
            return $this;
        }

        $fieldName = $this->getFullTableNameOrFullFieldName($field);

        if ($fieldName === '') {
            return $this;
        }

        $operator = '';
        $fieldValue = null;
        $cnt = count($args);

        if ($cnt === 1) {
            $operator = '=';
            $fieldValue = $args[0];
        } else if ($cnt > 1 && is_string($args[0])) {
            $operator = trim($args[0]);
            $fieldValue = $args[1];
        }

        if ($operator === '') {
            return $this;
        }

        if ($fieldValue === null) {
            $this->conditions[] = "$fieldName IS NULL";
            return $this;
        }

        if ($fieldValue instanceof Expression) {
            $fieldValue = $fieldValue->getExpr();
        }

        if (!is_int($fieldValue) && !is_float($fieldValue) && !is_string($fieldValue)) {
            return $this;
        }

        $this->conditions[] = "$fieldName $operator ?";
        $this->params[] = $fieldValue;
        return $this;
    }

    /**
     * @param string|Expression $field
     * @param bool $not
     * @return QueryBuilder
     */
    public function whereNull($field, bool $not = false): self
    {
        $fieldName = $this->getFullTableNameOrFullFieldName($field);

        if ($fieldName === '') {
            return $this;
        }

        $this->conditions[] = $not ? "$fieldName IS NOT NULL" : "$fieldName IS NULL";
        return $this;
    }

    /**
     * @param string|Expression $field
     * @return QueryBuilder
     */
    public function whereNotNull($field): self
    {
        return $this->whereNull($field, true);
    }

    /**
     * @param string|Expression $field
     * @param bool $not
     * @return QueryBuilder
     */
    public function whereBlank($field, bool $not = false): self
    {
        $fieldName = $this->getFullTableNameOrFullFieldName($field);

        if ($fieldName === '') {
            return $this;
        }

        $this->conditions[] = $not ? "$fieldName <> ''" : "$fieldName = ''";
        return $this;
    }

    /**
     * @param string|Expression $field
     * @return QueryBuilder
     */
    public function whereNotBlank($field): self
    {
        return $this->whereBlank($field, true);
    }

    /**
     * @param string|Expression $field
     * @param array $values
     * @param bool $not
     * @return QueryBuilder
     */
    public function whereIn($field, array $values, bool $not = false): self
    {
        $fieldName = $this->getFullTableNameOrFullFieldName($field);

        if ($fieldName === '') {
            return $this;
        }

        $paramList = collect($values)->filter(function ($it) {
            return is_int($it) || (is_string($it) && $it !== '');
        })->toArray();


        if (empty($paramList)) {
            return $this;
        }

        $whList = trim(str_repeat('?, ', count($paramList)), ', ');
        $operator = $not ? 'NOT IN' : 'IN';
        $this->conditions[] = "$fieldName $operator ($whList)";
        $this->params = array_merge($this->params, $paramList);
        return $this;
    }

    /**
     * @param string|Expression $field
     * @param array $values
     * @return QueryBuilder
     */
    public function whereNotIn($field, array $values): self
    {
        return $this->whereIn($field, $values, true);
    }

    /**
     * @param string|Expression $field
     * @param int|string|Expression $arg1
     * @param int|string|Expression $arg2
     * @param bool $not
     * @return QueryBuilder
     */
    public function whereBetween($field, $arg1, $arg2, bool $not = false): self
    {
        $fieldName = $this->getFullTableNameOrFullFieldName($field);

        if ($fieldName === '') {
            return $this;
        }

        if ($arg1 instanceof Expression) {
            $arg1 = $arg1->getExpr();
        }

        if (!is_int($arg1) && !is_string($arg1)) {
            return $this;
        }

        if ($arg2 instanceof Expression) {
            $arg2 = $arg2->getExpr();
        }

        if (!is_int($arg2) && !is_string($arg2)) {
            return $this;
        }

        $this->conditions[] = $not ? "$fieldName NOT BETWEEN ? AND ?" : "$fieldName BETWEEN ? AND ?";
        $this->params = array_merge($this->params, [$arg1, $arg2]);
        return $this;
    }

    /**
     * @param string|Expression $field
     * @param int|string|Expression $arg1
     * @param int|string|Expression $arg2
     * @return QueryBuilder
     */
    public function whereNotBetween($field, $arg1, $arg2): self
    {
        return $this->whereBetween($field, $arg1, $arg2);
    }

    /**
     * @param string|Expression $field
     * @param string $arg1
     * @param string $arg2
     * @param bool $onlyDate
     * @return QueryBuilder
     */
    public function whereTimeBetween($field, string $arg1, string $arg2, bool $onlyDate = false): self
    {
        $fieldName = $this->getFullTableNameOrFullFieldName($field);

        if ($fieldName === '') {
            return $this;
        }
        
        $d1 = StringUtils::toDateTime($arg1);
        $d2 = StringUtils::toDateTime($arg2);
        
        if (!($d1 instanceof DateTime) || !($d2 instanceof DateTime)) {
            return $this;
        }

        if ($onlyDate) {
            $arg1 = $d1->format(DateTimeFormat::DATE_ONLY);
            $arg2 = $d2->format(DateTimeFormat::DATE_ONLY);
        } else {
            $arg1 = $d1->format(DateTimeFormat::FULL);
            $arg2Format = $d2->format(DateTimeFormat::TIME_ONLY) === '00:00:00' ? 'Y-m-d 23:59:59' : DateTimeFormat::FULL;
            $arg2 = $d2->format($arg2Format);
        }

        return $this->whereBetween($fieldName, $arg1, $arg2);
    }

    /**
     * @param string|Expression $field
     * @param string $likeStr
     * @param bool $not
     * @return QueryBuilder
     */
    public function whereLike($field, string $likeStr, bool $not = false): self
    {
        if ($likeStr === '') {
            return $this;
        }

        $fieldName = $this->getFullTableNameOrFullFieldName($field);

        if ($fieldName === '') {
            return $this;
        }

        $likeStr =StringUtils::ensureLeft($likeStr, '%');
        $likeStr =StringUtils::ensureRight($likeStr, '%');
        $this->conditions[] = $not ? "$fieldName NOT LIKE ?" : "$fieldName LIKE ?";
        $this->params[] = $likeStr;
        return $this;
    }

    /**
     * @param string|Expression $field
     * @param string $likeStr
     * @return QueryBuilder
     */
    public function whereNotLike($field, string $likeStr): self
    {
        return $this->whereLike($field, $likeStr, true);
    }

    /**
     * @param string|Expression $field
     * @param string $regex
     * @param bool $not
     * @return QueryBuilder
     */
    public function whereRegexp($field, string $regex, bool $not = false): self
    {
        if ($regex === '') {
            return $this;
        }

        $fieldName = $this->getFullTableNameOrFullFieldName($field);

        if ($fieldName === '') {
            return $this;
        }

        $this->conditions[] = $not ? "$fieldName NOT REGEXP ?" : "$fieldName REGEXP ?";
        $this->params[] = $regex;
        return $this;
    }

    /**
     * @param string|Expression $field
     * @param string $regex
     * @return QueryBuilder
     */
    public function whereNotRegexp($field, string $regex): self
    {
        return $this->whereRegexp($field, $regex, true);
    }

    public function whereDate(string $fieldName, ...$args): self
    {
        $expr = sprintf('DATE(%s)', $this->getFullTableNameOrFullFieldName($fieldName));
        return $this->where(Expression::create($expr), ...$args);
    }

    public function whereYear(string $fieldName, ...$args): self
    {
        $expr = sprintf('YEAR(%s)', $this->getFullTableNameOrFullFieldName($fieldName));
        return $this->where(Expression::create($expr), ...$args);
    }

    public function whereMonth(string $fieldName, ...$args): self
    {
        $expr = sprintf('MONTH(%s)', $this->getFullTableNameOrFullFieldName($fieldName));
        return $this->where(Expression::create($expr), ...$args);
    }

    public function whereDay(string $fieldName, ...$args): self
    {
        $expr = sprintf('DAY(%s)', $this->getFullTableNameOrFullFieldName($fieldName));
        return $this->where(Expression::create($expr), ...$args);
    }

    public function whereTimestamp(string $fieldName, ...$args): self
    {
        $expr = sprintf('UNIX_TIMESTAMP(%s)', $this->getFullTableNameOrFullFieldName($fieldName));
        return $this->where(Expression::create($expr), ...$args);
    }

    public function whereSofeDelete(bool $flag): self
    {
        $schemas = DB::getTableSchema($this->tableName);

        $fieldNames = [
            'delete_at',
            'deleteAt'
        ];

        foreach ($schemas as $schema) {
            if (in_array($schema['fieldName'], $fieldNames) && strpos($schema['fieldType'], 'datetime') !== false) {
                if ($flag) {
                    return $this->whereNotNull($schema['fieldName']);
                } else {
                    return $this->whereNull($schema['fieldName']);
                }
            }
        }

        $fieldNames = [
            'del_flag',
            'delFlag'
        ];

        foreach ($schemas as $schema) {
            if (in_array($schema['fieldName'], $fieldNames) && strpos($schema['fieldType'], 'int') !== false) {
                if ($flag) {
                    return $this->where($schema['fieldName'], 1);
                } else {
                    return $this->where($schema['fieldName'], 0);
                }
            }
        }

        return $this;
    }

    public function whereRaw(string $expr, ...$args): self
    {
        if ($expr === '') {
            return $this;
        }

        $this->conditions[] = $expr;

        $params = array_filter($args, function ($it) {
            return is_int($it) || is_string($it);
        });

        if (!empty($params)) {
            $this->params = array_merge($this->params, array_values($params));
        }

        return $this;
    }

    /**
     * @param string|Expression $field
     * @param mixed ...$args
     * @return QueryBuilder
     */
    public function orWhere($field, ...$args): self
    {
        if (empty($args)) {
            return $this;
        }

        $fieldName = $this->getFullTableNameOrFullFieldName($field);

        if ($fieldName === '') {
            return $this;
        }

        $operator = '';
        $fieldValue = null;
        $cnt = count($args);

        if ($cnt === 1) {
            $operator = '=';
            $fieldValue = $args[0];
        } else if ($cnt > 1 && is_string($args[0])) {
            $operator = trim($args[0]);
            $fieldValue = $args[1];
        }

        if ($operator === '') {
            return $this;
        }

        if ($fieldValue === null) {
            $this->addOrCondition("$fieldName IS NULL");
            return $this;
        }

        if ($fieldValue instanceof Expression) {
            $fieldValue = $fieldValue->getExpr();
        }

        if (!is_int($fieldValue) && !is_string($fieldValue)) {
            return $this;
        }

        $this->addOrCondition("$fieldName $operator ?");
        $this->params[] = $fieldValue;
        return $this;
    }

    /**
     * @param string|Expression $field
     * @param bool $not
     * @return QueryBuilder
     */
    public function orWhereNull($field, bool $not = false): self
    {
        $fieldName = $this->getFullTableNameOrFullFieldName($field);

        if ($fieldName === '') {
            return $this;
        }

        $cond = $not ? "$fieldName IS NOT NULL" : "$fieldName IS NULL";
        $this->addOrCondition($cond);
        return $this;
    }

    /**
     * @param string|Expression $field
     * @return QueryBuilder
     */
    public function orWhereNotNull($field): self
    {
        return $this->orWhereNull($field, true);
    }

    /**
     * @param string|Expression $field
     * @param bool $not
     * @return QueryBuilder
     */
    public function orWhereBlank($field, bool $not = false): self
    {
        $fieldName = $this->getFullTableNameOrFullFieldName($field);

        if ($fieldName === '') {
            return $this;
        }

        $cond = $not ? "$fieldName <> ''" : "$fieldName = ''";
        $this->addOrCondition($cond);
        return $this;
    }

    /**
     * @param string|Expression $field
     * @return QueryBuilder
     */
    public function orWhereNotBlank($field): self
    {
        return $this->orWhereBlank($field, true);
    }

    /**
     * @param string|Expression $field
     * @param array $values
     * @param bool $not
     * @return QueryBuilder
     */
    public function orWhereIn($field, array $values, bool $not = false): self
    {
        $fieldName = $this->getFullTableNameOrFullFieldName($field);

        if ($fieldName === '') {
            return $this;
        }

        $paramList = collect($values)->filter(function ($it) {
            return is_int($it) || (is_string($it) && $it !== '');
        })->toArray();


        if (empty($paramList)) {
            return $this;
        }

        $whList = trim(str_repeat('?, ', count($paramList)), ', ');
        $operator = $not ? 'NOT IN' : 'IN';
        $cond = "$fieldName $operator ($whList)";
        $this->addOrCondition($cond);
        $this->params = array_merge($this->params, $paramList);
        return $this;
    }

    /**
     * @param string|Expression $field
     * @param array $values
     * @return QueryBuilder
     */
    public function orWhereNotIn($field, array $values): self
    {
        return $this->orWhereIn($field, $values, true);
    }

    /**
     * @param string|Expression $field
     * @param int|string|Expression $arg1
     * @param int|string|Expression $arg2
     * @param bool $not
     * @return QueryBuilder
     */
    public function orWhereBetween($field, $arg1, $arg2, bool $not = false): self
    {
        $fieldName = $this->getFullTableNameOrFullFieldName($field);

        if ($fieldName === '') {
            return $this;
        }

        if ($arg1 instanceof Expression) {
            $arg1 = $arg1->getExpr();
        }

        if (!is_int($arg1) && !is_string($arg1)) {
            return $this;
        }

        if ($arg2 instanceof Expression) {
            $arg2 = $arg2->getExpr();
        }

        if (!is_int($arg2) && !is_string($arg2)) {
            return $this;
        }

        $cond = $not ? "$fieldName NOT BETWEEN ? AND ?" : "$fieldName BETWEEN ? AND ?";
        $this->addOrCondition($cond);
        $this->params = array_merge($this->params, [$arg1, $arg2]);
        return $this;
    }

    /**
     * @param string|Expression $field
     * @param int|string|Expression $arg1
     * @param int|string|Expression $arg2
     * @return QueryBuilder
     */
    public function orWhereNotBetween($field, $arg1, $arg2): self
    {
        return $this->orWhereBetween($field, $arg1, $arg2);
    }

    /**
     * @param string|Expression $field
     * @param string $likeStr
     * @param bool $not
     * @return QueryBuilder
     */
    public function orWhereLike($field, string $likeStr, bool $not = false): self
    {
        if ($likeStr === '') {
            return $this;
        }

        $fieldName = $this->getFullTableNameOrFullFieldName($field);

        if ($fieldName === '') {
            return $this;
        }

        $cond = $not ? "$fieldName NOT LIKE ?" : "$fieldName LIKE ?";
        $this->addOrCondition($cond);
        $this->params[] = $likeStr;
        return $this;
    }

    /**
     * @param string|Expression $field
     * @param string $likeStr
     * @return QueryBuilder
     */
    public function orWhereNotLike($field, string $likeStr): self
    {
        return $this->orWhereLike($field, $likeStr, true);
    }

    /**
     * @param string|Expression $field
     * @param string $regex
     * @param bool $not
     * @return QueryBuilder
     */
    public function orWhereRegexp($field, string $regex, bool $not = false): self
    {
        if ($regex === '') {
            return $this;
        }

        $fieldName = $this->getFullTableNameOrFullFieldName($field);

        if ($fieldName === '') {
            return $this;
        }

        $cond = $not ? "$fieldName NOT REGEXP ?" : "$fieldName REGEXP ?";
        $this->addOrCondition($cond);
        $this->params[] = $regex;
        return $this;
    }

    /**
     * @param string|Expression $field
     * @param string $regex
     * @return QueryBuilder
     */
    public function orWhereNotRegexp($field, string $regex): self
    {
        return $this->orWhereRegexp($field, $regex, true);
    }

    public function orWhereDate(string $fieldName, ...$args): self
    {
        $expr = sprintf('DATE(%s)', $this->getFullTableNameOrFullFieldName($fieldName));
        return $this->orWhere(Expression::create($expr), ...$args);
    }

    public function orWhereYear(string $fieldName, ...$args): self
    {
        $expr = sprintf('YEAR(%s)', $this->getFullTableNameOrFullFieldName($fieldName));
        return $this->orWhere(Expression::create($expr), ...$args);
    }

    public function orWhereMonth(string $fieldName, ...$args): self
    {
        $expr = sprintf('MONTH(%s)', $this->getFullTableNameOrFullFieldName($fieldName));
        return $this->orWhere(Expression::create($expr), ...$args);
    }

    public function orWhereDay(string $fieldName, ...$args): self
    {
        $expr = sprintf('DAY(%s)', $this->getFullTableNameOrFullFieldName($fieldName));
        return $this->orWhere(Expression::create($expr), ...$args);
    }

    public function orWhereTimestamp(string $fieldName, ...$args): self
    {
        $expr = sprintf('UNIX_TIMESTAMP(%s)', $this->getFullTableNameOrFullFieldName($fieldName));
        return $this->orWhere(Expression::create($expr), ...$args);
    }

    public function orWhereSofeDelete(bool $flag): self
    {
        $schemas = DB::getTableSchema($this->tableName);

        $fieldNames = [
            'delete_at',
            'deleteAt'
        ];

        foreach ($schemas as $schema) {
            if (in_array($schema['fieldName'], $fieldNames) && strpos($schema['fieldType'], 'datetime') !== false) {
                if ($flag) {
                    return $this->orWhereNotNull($schema['fieldName']);
                } else {
                    return $this->orWhereNull($schema['fieldName']);
                }
            }
        }

        $fieldNames = [
            'del_flag',
            'delFlag'
        ];

        foreach ($schemas as $schema) {
            if (in_array($schema['fieldName'], $fieldNames) && strpos($schema['fieldType'], 'int') !== false) {
                if ($flag) {
                    return $this->orWhere($schema['fieldName'], 1);
                } else {
                    return $this->orWhere($schema['fieldName'], 0);
                }
            }
        }

        return $this;
    }

    public function orWhereRaw(string $expr, ...$args): self
    {
        if ($expr === '') {
            return $this;
        }

        $this->addOrCondition($expr);

        $params = collect($args)->filter(function ($it) {
            return is_int($it) || is_string($it);
        })->toArray();

        if (!empty($params)) {
            $this->params = array_merge($this->params, $params);
        }

        return $this;
    }

    /**
     * @param string|array|Expression $orderBy
     * @return QueryBuilder
     */
    public function orderBy($orderBy): self
    {
        $rules = [];

        if (is_string($orderBy)) {
            foreach (preg_split(Regexp::COMMA_SEP, $orderBy) as $value) {
                $rule = $this->buildOrderByItem($value);

                if ($rule === null) {
                    continue;
                }

                $rules[] = $rule;
            }
        } else if (is_array($orderBy)) {
            foreach ($orderBy as $key => $value) {
                $rule = null;

                if (is_string($key) && is_string($value)) {
                    $value = empty($value) ? 'ASC' : $value;
                    $rule = $this->buildOrderByItem($key . " " . $value);
                } else if (is_string($value) && !empty($value)) {
                    $rule = $this->buildOrderByItem("$value ASC");
                } else if ($value instanceof Expression) {
                    $rule = $this->buildOrderByItem($value);
                }

                if ($rule === null) {
                    continue;
                }

                $rules[] = $rule;
            }
        } else if ($orderBy instanceof Expression) {
            $rules[] = $orderBy->getExpr();
        }

        if (empty($rules)) {
            return $this;
        }

        $this->orderBy = $rules;
        return $this;
    }

    /**
     * @param string|array $groupBy
     * @return QueryBuilder
     */
    public function groupBy($groupBy): self
    {
        $rules = [];

        if (is_string($groupBy)) {
            foreach (preg_split(Regexp::COMMA_SEP, $groupBy) as $value) {
                $rule = $this->buildGroupByItem($value);

                if ($rule === null) {
                    continue;
                }

                $rules[] = $rule;
            }
        } else if (is_array($groupBy)) {
            foreach ($groupBy as $value) {
                $rule = $this->buildGroupByItem($value);

                if ($rule === null) {
                    continue;
                }

                $rules[] = $rule;
            }
        }

        if (empty($rules)) {
            return $this;
        }

        $this->groupBy = $rules;
        return $this;
    }

    /**
     * @param int|string ...$args
     * @return QueryBuilder
     */
    public function limit(...$args): self
    {
        if (empty($args)) {
            return $this;
        }

        $cnt = count($args);

        if ($cnt === 1 && is_string($args[0])) {
            $parts = [];

            foreach (preg_split(Regexp::COMMA_SEP, $args[0]) as $p) {
                $p = trim($p);

                if (empty($p)) {
                    continue;
                }

                $parts[] = $p;
            }

            return empty($parts) ? $this : $this->limit(...$parts);
        }

        if ($cnt === 1 && is_int($args[0]) && $args[0] > 0) {
            $this->limits = [$args[0]];
            return $this;
        }

        if ($cnt > 1) {
            $offset = null;

            if (is_int($args[0])) {
                $offset = $args[0];
            } else if (is_string($args[0]) &&StringUtils::isInt($args[0])) {
                $offset = (int)$args[0];
            }

            if (!is_int($offset) || $offset < 0) {
                return $this;
            }

            $limit = null;

            if (is_int($args[1])) {
                $limit = $args[1];
            } else if (is_string($args[1]) &&StringUtils::isInt($args[1])) {
                $limit = (int)$args[1];
            }

            if (!is_int($limit) || $limit < 1) {
                return $this;
            }

            $this->limits = [$offset, $limit];
        }

        return $this;
    }

    public function forPage(int $page, int $pageSize = 20): self
    {
        if ($page < 1 || $pageSize < 1) {
            return $this;
        }

        return $this->limit(($page - 1) * $pageSize, $pageSize);
    }

    public function buildForSelect(): array
    {
        $params = empty($this->params) ? [] : $this->params;
        $sb = ['SELECT '];
        $sb[] = empty($this->fields) ? '*' : implode(', ', $this->fields);
        $sb[] = " FROM $this->tableName";

        if (!empty($this->joins)) {
            $sb[] = ' ' . implode(' ', $this->joins);
        }

        if (!empty($this->conditions)) {
            $sb[] = ' WHERE ' . implode(' AND ', $this->conditions);
        }

        if (!empty($this->orderBy)) {
            $sb[] = ' ORDER BY ' . implode(', ', $this->orderBy);
        }

        if (!empty($this->groupBy)) {
            $sb[] = ' GROUP BY ' . implode(', ', $this->groupBy);
        }

        $n1 = count($this->limits);

        if ($n1 === 1) {
            $sb[] = ' LIMIT ?';
            $params[] = $this->limits[0];
        } else if ($n1 === 2) {
            $sb[] = ' LIMIT ?, ?';
            $params[] = $this->limits[0];
            $params[] = $this->limits[1];
        }

        return [implode('', $sb), $params];
    }

    public function buildForCount(string $countField = '*'): array
    {
        $regex = '/[\x20\t]+as[\x20\t]+/i';
        $parts = [];

        foreach (preg_split($regex, $countField) as $p) {
            $p = trim($p);

            if (empty($p)) {
                continue;
            }

            $parts[] = $p;
        }

        $cnt = count($parts);
        $fieldName = 'COUNT(*)';

        if ($cnt === 1) {
            $fieldName = sprintf('COUNT(%s)', $this->getFullTableNameOrFullFieldName($parts[0]));
        } else if ($cnt > 1) {
            $fieldName = sprintf(
                'COUNT(%s) AS %s',
                $this->getFullTableNameOrFullFieldName($parts[0]),
                $parts[1]
            );
        }

        $sb = ["SELECT $fieldName FROM $this->tableName"];

        if (!empty($this->joins)) {
            $sb[] = ' ' . implode(' ', $this->joins);
        }

        if (!empty($this->conditions)) {
            $sb[] = ' WHERE ' . implode(' AND ', $this->conditions);
        }

        if (!empty($this->orderBy)) {
            $sb[] = ' ORDER BY ' . implode(', ', $this->orderBy);
        }

        if (!empty($this->groupBy)) {
            $sb[] = ' GROUP BY ' . implode(', ', $this->groupBy);
        }

        $sb[] = " LIMIT 1";
        return [implode('', $sb), $this->params];
    }

    public function buildForSum(string $sumField): array
    {
        $regex = '/[\x20\t]+as[\x20\t]+/i';
        $parts = [];

        foreach (preg_split($regex, $sumField) as $p) {
            $p = trim($p);

            if (empty($p)) {
                continue;
            }

            $parts[] = $p;
        }

        $cnt = count($parts);

        /** @var string $fieldName */

        if ($cnt === 1) {
            $fieldName = sprintf('SUM(%s)', $this->getFullTableNameOrFullFieldName($parts[0]));
        } else if ($cnt > 1) {
            $fieldName = sprintf(
                'SUM(%s) AS %s',
                $this->getFullTableNameOrFullFieldName($parts[0]),
                $parts[1]
            );
        }

        $sb = ["SELECT $fieldName FROM $this->tableName"];

        if (!empty($this->conditions)) {
            $sb[] = ' WHERE ' . implode(' AND ', $this->conditions);
        }

        if (!empty($this->orderBy)) {
            $sb[] = ' ORDER BY ' . implode(', ', $this->orderBy);
        }

        if (!empty($this->groupBy)) {
            $sb[] = ' GROUP BY ' . implode(', ', $this->groupBy);
        }

        $sb[] = " LIMIT 1";
        return [implode('', $sb), $this->params];
    }

    public function buildForInsert(array $data): array
    {
        $schemas = DB::getTableSchema($this->tableName);

        $fieldNames = [
            'ctime',
            'create_at',
            'createAt',
            'create_time',
            'createTime'
        ];

        foreach ($schemas as $schema) {
            if (in_array($schema['fieldName'], $fieldNames) && strpos($schema['fieldType'], 'datetime') !== false) {
                $data[$schema['fieldName']] = date(DateTimeFormat::FULL);
                break;
            }
        }

        $columns = [];
        $values = [];
        $params = [];

        foreach ($data as $key => $value) {
            $columns[] = $this->getFullTableNameOrFullFieldName($key);

            if ($value === null) {
                $values[] = 'null';
                continue;
            } else if ($value instanceof Expression) {
                $values[] = $value->getExpr();
                continue;
            }

            $values[] = '?';
            $params[] = $value;
        }

        $sql = "INSERT INTO $this->tableName (%s) VALUES (%s)";
        $sql = sprintf($sql, implode(', ', $columns), implode(', ', $values));
        return [$sql, $params];
    }

    public function buildForUpdate(array $data): array
    {
        $schemas = DB::getTableSchema($this->tableName);

        $fieldNames = [
            'update_at',
            'updateAt'
        ];

        foreach ($schemas as $schema) {
            if (in_array($schema['fieldName'], $fieldNames) && strpos($schema['fieldType'], 'datetime') !== false) {
                $data[$schema['fieldName']] = date(DateTimeFormat::FULL);
                break;
            }
        }

        $sets = [];
        $params = [];

        foreach ($data as $key => $value) {
            $fieldName = $this->getFullTableNameOrFullFieldName($key);

            if ($value === null) {
                $sets[] = "$fieldName = null";
                continue;
            } else if ($value instanceof Expression) {
                $sets[] = "$fieldName = " . $value->getExpr();
                continue;
            }

            $sets[] = "$fieldName = ?";
            $params[] = $value;
        }

        $sb = ["UPDATE $this->tableName SET "];
        $sb[] = implode(', ', $sets);

        if (!empty($this->conditions)) {
            $sb[] = ' WHERE ' . implode(' AND ', $this->conditions);
        }

        if (!empty($this->params)) {
            $params = array_merge($params, $this->params);
        }

        return [implode('', $sb), $params];
    }

    public function buildForTruncate(): array
    {
        $sql = "TRUNCATE TABLE $this->tableName";
        return [$sql, []];
    }

    public function buildForDelete(): array
    {
        $sb = ["DELETE FROM $this->tableName"];

        if (!empty($this->conditions)) {
            $sb[] = ' WHERE ' . implode(' AND ', $this->conditions);
        }

        return [implode('', $sb), $this->params];
    }

    /**
     * @param string|array|null $fields
     * @param mixed $txm
     * @return Collection
     */
    public function get($fields = null, $txm = null): Collection
    {
        if ($fields !== null) {
            $this->fields($fields);
        }

        list($sql, $params) = $this->buildForSelect();
        return DB::selectBySql($sql, $params, $txm);
    }

    /**
     * @param string|array|null $fields
     * @param mixed $txm
     * @return array|null
     */
    public function first($fields = null, $txm = null): ?array
    {
        if ($fields !== null) {
            $this->fields($fields);
        }

        $this->limit(1);
        list($sql, $params) = $this->buildForSelect();
        return DB::firstBySql($sql, $params, $txm);
    }

    public function value(string $columnName, $txm = null)
    {
        $data = $this->first($columnName, $txm);

        if (!ArrayUtils::isAssocArray($data)) {
            return null;
        }

        return $data[$columnName] ?? null;
    }

    public function intValue(string $columnName, int $defaultValue = PHP_INT_MIN, $txm = null): int
    {
        return Cast::toInt($this->value($columnName, $txm), $defaultValue);
    }

    public function stringValue(string $columnName, $txm = null): string
    {
        return Cast::toString($this->value($columnName, $txm));
    }

    public function count(string $countField = '*', $txm = null): int
    {
        list($sql, $params) = $this->buildForCount($countField);
        return DB::countBySql($sql, $params, $txm);
    }

    public function exists(string $countField = '*', $txm = null): bool
    {
        return $this->count($countField, $txm) > 0;
    }

    public function insert(array $data, $txm = null): int
    {
        list($sql, $prams) = $this->buildForInsert($data);
        return DB::insertBySql($sql, $prams, $txm);
    }

    public function update(array $data, $txm = null): int
    {
        list($sql, $params) = $this->buildForUpdate($data);
        return DB::updateBySql($sql, $params, $txm);
    }

    public function sofeDelete($txm = null): void
    {
        $updateSet = [];
        $schemas = DB::getTableSchema($this->tableName);

        $fieldNames = [
            'delete_at',
            'deleteAt'
        ];

        foreach ($schemas as $schema) {
            if (in_array($schema['fieldName'], $fieldNames) && strpos($schema['fieldType'], 'datetime') !== false) {
                $updateSet[$schema['fieldName']] = date(DateTimeFormat::FULL);
                break;
            }
        }

        $fieldNames = [
            'del_flag',
            'delFlag'
        ];

        foreach ($schemas as $schema) {
            if (in_array($schema['fieldName'], $fieldNames) && strpos($schema['fieldType'], 'int') !== false) {
                $updateSet[$schema['fieldName']] = 1;
                break;
            }
        }

        if (empty($updateSet)) {
            return;
        }

        $this->update($updateSet, $txm);
    }

    public function delete($txm = null): int
    {
        list($sql, $params) = $this->buildForDelete();
        return DB::deleteBySql($sql, $params, $txm);
    }

    /**
     * @param string $fieldName
     * @param int|float|string $num
     * @param mixed $txm
     * @return int
     */
    public function incr(string $fieldName, $num, $txm = null): int {
        if (is_float($num) || is_string($num)) {
            $num = bcadd($num, 0, 2);

            if (bccomp($num, 0, 2) !== 1) {
                return 0;
            }
        } else if (!is_int($num)) {
            return 0;
        }

        return $this->update([$fieldName => DB::raw("$fieldName + $num")], $txm);
    }

    /**
     * @param string $fieldName
     * @param int|float|string $num
     * @param null $txm
     * @return int
     */
    public function decr(string $fieldName, $num, $txm = null): int {
        if (is_float($num) || is_string($num)) {
            $num = bcadd($num, 0, 2);

            if (bccomp($num, 0, 2) !== 1) {
                return 0;
            }
        } else if (!is_int($num)) {
            return 0;
        }

        return $this->update([$fieldName => DB::raw("$fieldName - $num")], $txm);
    }

    /**
     * @param string $fieldName
     * @param null $txm
     * @return int|float|string
     */
    public function sum(string $fieldName, $txm = null)
    {
        list($sql, $params) = $this->buildForSum($fieldName);
        return DB::sumBySql($sql, $params, $txm);
    }

    /**
     * @param string|Expression $arg0
     * @return string
     */
    private function getFullTableNameOrFullFieldName($arg0): string
    {
        if ($arg0 instanceof Expression) {
            return $arg0->getExpr();
        }

        if (!is_string($arg0)) {
            return '';
        }

        $regex = '/[\x20\t]+as[\x20\t]+/i';
        $parts = [];

        foreach (preg_split($regex, $arg0) as $p) {
            $p = trim($p);

            if (empty($p)) {
                continue;
            }

            $parts[] = $p;
        }

        if (empty($parts)) {
            return '';
        }

        return count($parts) === 1 ? $parts[0] : "$parts[0] AS $parts[1]";
    }

    /**
     * @param string|Expression $field
     * @return int
     */
    private function findFieldIndex($field): int
    {
        foreach ($this->fields as $i => $value) {
            if ($this->getFullTableNameOrFullFieldName($field) === $value) {
                return $i;
            }
        }

        return -1;
    }

    private function addOrCondition(string $cond): void
    {
        if (empty($this->conditions)) {
            $this->conditions[] = $cond;
        }

        $last = array_pop($this->conditions);
        $this->conditions[] = "($last OR $cond)";
    }

    /**
     * @param string|Expression $arg0
     * @return string|null
     */
    private function buildOrderByItem($arg0): ?string
    {
        if ($arg0 instanceof Expression) {
            return $arg0->getExpr();
        }

        if (!is_string($arg0)) {
            return null;
        }

        $parts = [];

        foreach (preg_split(Regexp::SPACE_SEP, $arg0) as $p) {
            $p = trim($p);

            if (empty($p)) {
                continue;
            }

            $parts[] = $p;
        }

        $cnt = count($parts);

        if ($cnt < 1) {
            return null;
        }

        $fieldName = $this->getFullTableNameOrFullFieldName($parts[0]);

        if ($fieldName === '') {
            return null;
        }

        $directions = ['ASC', 'DESC'];

        if ($cnt > 1 && in_array(strtoupper($parts[1]), $directions)) {
            $direction = strtoupper($parts[1]);
        } else {
            $direction = 'ASC';
        }

        return "$fieldName $direction";
    }

    /**
     * @param string|Expression $arg0
     * @return string|null
     */
    private function buildGroupByItem($arg0): ?string
    {
        if ($arg0 instanceof Expression) {
            return $arg0->getExpr();
        }

        if (!is_string($arg0)) {
            return null;
        }

        $parts = [];

        foreach (preg_split(Regexp::SPACE_SEP, $arg0) as $p) {
            $p = trim($p);

            if (empty($p)) {
                continue;
            }

            $parts[] = $p;
        }

        $cnt = count($parts);

        if ($cnt < 1) {
            return null;
        }

        $fieldName = $this->getFullTableNameOrFullFieldName($parts[0]);
        return $fieldName === '' ? null : $fieldName;
    }
}

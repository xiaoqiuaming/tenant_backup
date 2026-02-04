#include "ob_define.h"
#include <yylog.h>
#ifndef _OCEANBASE_COMMON_OB_DECIMAL_HELPER_
#define _OCEANBASE_COMMON_OB_DECIMAL_HELPER_
namespace oceanbase
{
  namespace tests
  {
    namespace common
    {
      class TestObjDecimalHelper;
    }
  }
  namespace common
  {
    const static int64_t OB_MAX_DECIMAL_WIDTH = 27;
    const static int64_t OB_MAX_PRECISION_WIDTH = 9;
    const static int64_t OB_MAX_PRECISION_NUMBER = 999999999L;
    const static int64_t OB_DECIMAL_MULTIPLICATOR = 10;
    const static int64_t OB_MAX_DIGIT = 9;
    const static int64_t OB_MAX_DIGIT_ASC = '9';
    const static int64_t OB_INT_BINARY_LENGTH = 64;
    const static int64_t OB_INT_HALF_LENGTH = 32;
    const static int64_t OB_LEFT_PART_MAST_WITH_SIGN = 0xefff0000;
    const static int64_t OB_RIGHT_PART_MAST = 0x0000ffff;
    const static int64_t OB_LEFT_PART_MAST_WITHOUT_SIGN = 0xffff0000;
    const static int64_t OB_ARRAY_SIZE = 64;
    const static int64_t OB_DECIMAL_EQUAL = 0;
    const static int64_t OB_DECIMAL_LESS = -1;
    const static int64_t OB_DECIMAL_BIG = 1;
    //�ڴ��������ʹ��
    /*�����DEFAULT����ô���ʱ���������棬�������ݽض�Ϊ�ÿ����µ���󶨵���
      �����STRICT,��ô��ֱ�ӷ��ش��󣬲��������� */
    enum ObSqlMode
    {
      SQL_DEFAULT = 0,
      SQL_STRICT,
    };

    //�ڴ���������߳�0��ʱ��ʹ��
    /*error_division_by_zero��ָ������Ϊ0ʱ�Ĵ���������
      ���һ�����error_division_by_zeroΪ�棬����sql_modeΪstrictʱ����ֱ�ӷ��ش���
      ����������error_division_by_zeroΪ�棬��sql_modeΪdefaultʱ���᷵�ؾ��棬���Ҽ�����ΪNULL��
      ��������£����᷵�ؾ��棬���Ҽ�����ΪNULL��*/
    struct ObTableProperty
    {
        ObSqlMode mode;
        bool error_division_by_zero;

        static ObTableProperty& getInstance()
        {
          static ObTableProperty instance;
          return instance;
        }

        //private:
        ObTableProperty()
        {
          mode = SQL_DEFAULT;
          error_division_by_zero = true;
        }
    };

    class ObObj;
    class ObDecimalHelper
    {
      public:
        friend class ObObj;
        friend class tests::common::TestObjDecimalHelper;
        struct meta
        {
            int8_t sign_:1;
            int8_t precision_:7;
            int8_t width_:8;
        };

        union meta_union
        {
            int16_t reserved_;
            meta value_;
        };

        struct ObDecimal
        {
            int64_t integer;
            int64_t fractional;
            meta_union meta;
        };

      public:
        ///@fn �������Ϊwidth���������������width = 4����ô����ֵ��9999
        ///@param[out] ���ؿ���Ϊwidth���������
        static int get_max_express_number(const int64_t width, int64_t &value);

        ///@fn �������Ϊprecision����С����,����width = 4,��ô����ֵ��1000
        static int get_min_express_number(const int64_t width, int64_t &number);

        ///@fnʵ��С�����ֵ���������
        ///@param[in/out]fractional С�����ֵ���ֵ
        ///@param[out]���������Ƿ����������Ľ�λ
        ///@param[in]precision �����������Ҫ����ľ���
        static int round_off(int64_t &fractional, bool &is_carry, const int64_t precision);

        ///@fn ������Ϊsrc_pre��С�����ְ���dest_pre�ľ�����������������
        ///@param fractional[in/out] С�����ֵ�ֵ
        ///@param is_carry[out] �Ƿ���������λ�Ľ�λ
        ///@param src_pre[in] ����С���ľ���
        ///@param dest_pre[src] ת���Ժ�С���ľ���
        static int round_off(int64_t &fractional, bool &is_carry, const int64_t src_pre, const int64_t dest_pre);

        ///@fn ���ַ���ת���ɶ��������������ֺ�С������
        ///@param value[in] �ַ����׵�ַ
        ///@param length[in] �������ַ����ĳ���
        ///@param integer[out] ���ַ����н�����������������
        ///@param fractional[out] ���ַ����н���������С������
        ///@param precision[out] ���ַ����н���������С���ĳ���
        ///@param sign[out] ���������������ķ��ţ�trueΪ���ţ�falseΪ����
        static int string_to_int(const char* value,
                                 const int64_t length,
                                 int64_t &integer,
                                 int64_t &fractional,
                                 int64_t &precision,
                                 bool &sign);

        ///@fn ʵ����������������ֵ���������
        ///@param decimal[in/out] ������
        ///@param add_decimal[in] ����
        ///@param gb_table_property[in] ����Ĵ�������
        static int ADD(ObDecimal &decimal,
                       const ObDecimal &add_decimal,
                       const ObTableProperty &gb_table_property);

        ///@fn ʵ������int64_tֵ�ĳ˷�
        ///@param product �˻���ŵĵ�ַ
        ///@param array_size produce�˻�����Ĵ�С������Ҫλ4
        // static int MULT(const int64_t multiplier, const int64_t multiplicand, int32_t *product, const int64_t array_size);

        ///@fn �������������ĳ˷�
        ///@param decimal[in] ������
        ///@param multiplicand [in] ����
        ///@param product_int [out] �˻�������
        ///@param product_fra [out] �˻���С������
        ///@param sign [out] �˻��ķ���
        ///@param gb_table_property[in] ����Ĵ�������
        static int MUL(const ObDecimal &decimal,
                       int64_t multiplicand,
                       int64_t &product_int,
                       int64_t &product_fra,
                       bool &sign,
                       const ObTableProperty &gb_table_property);

        ///@fn ʵ�ֶ������������ĳ���
        ///@param decimal[in] �������ı�����
        ///@param divisor[in] ����
        ///@param consult_int[out] �̵���������
        ///@param consult_fra[out] �̵�С������
        ///@param sign[out] �̵ķ���
        ///@param gb_table_property[in] ����Ĵ�������
        static int DIV(const ObDecimal &decimal,
                       int64_t divisor,
                       int64_t &consult_int,
                       int64_t &consult_fra,
                       bool &sign,
                       const ObTableProperty &gb_table_property);

        ///@fn�Ƚ������������Ĵ�С,
        ///@����ֵ��4��,0��ʾ�൱��1��ʾ���ڣ�-1��ʾС��,-2��ʾ�����������
        static int decimal_compare(ObDecimal decimal, ObDecimal other_decimal);

      private:

        //@fn Ϊ�˽���С���ıȽ�,��С���ľ��Ƚ�������
        ///@param fractional[in/out] С�����ֵ�ֵ
        ///@param precision[in] Ӧ�������ľ���
        static int promote_fractional(int64_t &fractional, const int64_t precision);

        ///@fnʵ�ֶ������������ĳ˷�,���������������
        ///@param integer[in] ����������������
        ///@param fractional[in]��������С������
        ///@param precision[in] �������ľ���
        ///@param multiplicand[in] ������
        ///@param length[in] ����ĳ��ȣ�����С��64
        static int array_mul(const int64_t integer,
                             const int64_t fractional,
                             const int64_t precision,
                             const int64_t multiplicand,
                             int8_t *product,
                             const int64_t length);

        ///@fnʵ�ֶ������������ĳ���,�������������
        ///@param integer[in] ����������������
        ///@param fractional[in] ��������С������
        ///@param precision[in] С���ľ���
        ///@param divisor[in] ������Ϊ�Ǹ���
        ///@param consult[out] ����Ĵ�ŵط�
        ///@param consult_len[in] ����consult�Ĵ�С
        static int array_div(const int64_t integer,
                             const int64_t fractional,
                             const int64_t precision,
                             const int64_t divisor,
                             int8_t *consult,
                             const int64_t consult_len);

        ///@fn�����жϼӷ�����ʱ�����Ƿ����,���������򰴶�������ģʽ����ת����С�������������λ
        static int is_over_flow(const int64_t width,
                                const int64_t precision,
                                int64_t &integer,
                                int64_t &fractional,
                                const ObTableProperty &gb_table_property);

        ///@fn��������������ģʽʱ��������й�񻯣�С��������������������
        static int decimal_format(const int64_t width,
                                  const int64_t precision,
                                  int64_t &integer,
                                  int64_t &fractional,
                                  const ObTableProperty &gb_table_property);

        ///@fn ����ַ�����ʾ�Ķ������Ƿ�Ϸ�
        ///����Ϸ����򷵻��ַ������Ƿ������λ������λ�������Ǹ����Ƿ���С������
        ///@param value[in] �����������ַ���
        ///@param length[in] ��value��ǰlength���ַ�Ϊ�������ַ�
        ///@param has_fractional[out] �����Ƿ����С������
        ///@param has_sign[out] �����Ƿ������λ
        ///@param sign[out] ����λ�������Ǹ�
        static int check_decimal_str(const char *value,
                                     const int64_t length,
                                     bool &has_fractional,
                                     bool &has_sign,
                                     bool &sign);
        static int check_decimal(const ObDecimal &decimal);

        ///@fn ��һ��λ��Ϊvalue_len������д��һ����СΪsize��int8_t������
        ///@param array[out] д���int8_t����
        ///@param size[in] ����Ĵ�С
        ///@param value_len[out] Ҫд��������λ��
        ///@param value[in] Ҫд�������
        static int int_to_byte(int8_t* array, const int64_t size, int64_t &value_len, const int64_t value, const bool in_head);
        ///@fn ͳ������value���ɼ�λ�������
        static int get_digit_number(const int64_t value, int64_t &number);

        ///@fn ��һ��λ��Ϊvalue_len������д��һ����СΪsize��int8_t������
        ///@param value[out] Ҫȡ��������
        ///@param value_len[in] ������λ��
        ///@param array[in] �洢������int8_t����
        static int byte_to_int(int64_t &value, const int8_t *array, const int64_t value_len);

        ///@fn ʵ������int8_t����ļӷ������У�adder������Ҫ����posλ�Ժ󣬽�����ӣ���λ��0��ʵ�ֳ˷��е���λ���
        ///@param size[in] ��������Ĵ�С��Ϊsize,size �Ĵ�СOB_ARRAY_SIZE�����治�����
        static int byte_add(int8_t *add, const int8_t *adder, const int64_t size, const int64_t pos);
    };
  }
}

#endif
